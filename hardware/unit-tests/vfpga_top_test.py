"""End-to-end tests for the shipping OASIS vFPGA top (hardware/src/vfpga_top.svh).

Deliberately no `alternative_vfpga_top_file`: this is the only test in the repo that
elaborates and drives the design that actually gets synthesised. It covers the pieces no
per-module test reaches -- the register map produced by GlobalConfig, ReadReqConfig +
LocalRead pulling a column chunk out of host memory, the ColumnChunkDecoder, MemConfig
buffer handling, and the PairedOutputWriter that merges each decoder's values and string
heap onto one physical channel.

Adapted from parcore/hardware/unit-tests/vfpga_top_test.py. The difference is the front
end: ParCore's top is *pushed* input over axis_host_recv, while OASIS *pulls* it -- the
chunk is staged in simulated host memory and a (vaddr, len) pair is enqueued on
ReadReqConfig, which is what the DuckDB extension does on real hardware.

Stream numbering follows PairedOutputWriter: decoder I writes its values on logical stream
2I and its string heap on 2I + 1, both sharing physical channel I. The heap stays completely
silent for anything that is not a german string chunk, so no transfer and no interrupt are
expected on it there.

This is the local (non-RDMA) configuration: build it with
`scripts/setup_simulation.sh --decoders 2`.
"""

from dataclasses import dataclass
from typing import Callable, Optional

from coyote_test import constants, fpga_register, fpga_stream

import libstf_utils.memory_manager as memory_manager

from libstf_utils.common import MAX_SUPPORTED_N_STREAMS
from libstf_utils.output_writer_test_case import OutputWriterTestCase
from parcore_utils.fpga_configuration import ColumnChunkDecoderConfig, Compression

from column_chunk_decoder_test import (
    _ColumnChunk,
    _PLAIN_OUTPUT,
    _RLE_OUTPUT,
    _heap_bytes,
    _german_records,
    _parquet_chunk,
    make_plain_data,
    make_string_data,
    make_tricky,
)

# FPGAOutputMemoryManager caps stream ids at MAX_NUMBER_STREAMS, which used to be both the
# number of physical AXI channels and the number of output streams. PairedOutputWriter puts
# two logical streams on each channel, so the real bound is the width of the interrupt's
# stream id field. Widening the guard is enough -- nothing else in the manager assumes one
# stream per channel.
memory_manager.MAX_NUMBER_STREAMS = MAX_SUPPORTED_N_STREAMS

COLUMN_CHUNK_DECODER_CONFIG_ID = 0x5C19F934407065BD
READ_REQ_CONFIG_ID = 0x2F966A70F04C0E93

# oasis::NUM_READ_REQ_CONFIG_REGS -- (vaddr, len, ctid) per stream.
READ_REQ_REGS_PER_STREAM = 3

# vfpga_top's DATABEAT_SIZE = AXI_DATA_BITS / 8. LocalRead's request generator aligns read
# addresses to this.
DATABEAT_SIZE = 64

def values_stream(decoder: int) -> int:
    return 2 * decoder


def heap_stream(decoder: int) -> int:
    return 2 * decoder + 1


def _register_value(value: int) -> bytearray:
    return bytearray(value.to_bytes(8, "little"))


@dataclass
class _Job:
    """One column chunk to enqueue on one decoder, plus what it must produce.

    String chunks are finished off late: the addresses embedded in their german_str_t
    records point into the heap buffer the writer will fill, and that buffer is only handed
    out once the simulation is running. `finish` therefore takes the heap base address and
    returns the chunk together with its expected values. The raw heap bytes are independent
    of that address, so they are known up front and are what the allocation is registered
    against.
    """

    finish: Callable[[int], "tuple[_ColumnChunk, list]"]
    heap_bytes: Optional[bytearray] = None

    @property
    def is_string(self) -> bool:
        return self.heap_bytes is not None


def fixed_job(chunk: _ColumnChunk, values: list) -> _Job:
    return _Job(finish=lambda _heap_addr: (chunk, values))


def string_job(strings: "list[bytes]") -> _Job:
    def finish(heap_addr: int):
        return make_string_data(strings, heap_addr), _german_records(strings, heap_addr)

    return _Job(finish=finish, heap_bytes=_heap_bytes(strings))


def plain_job(items: list, stream_type=fpga_stream.StreamType.SIGNED_INT_64) -> _Job:
    return fixed_job(make_plain_data(items, stream_type), items)


def parquet_job(filename: str, values: list) -> _Job:
    return fixed_job(_parquet_chunk(filename), values)


class VFPGATopTestCase(OutputWriterTestCase):
    # Deliberately no alternative_vfpga_top_file: this drives hardware/src/vfpga_top.svh.
    debug_mode = True

    # Local mode has no RDMA bypass stream, so every physical channel carries a decoder.
    # The RDMA counterpart overrides this because its final channel is the bypass path.
    num_decoders = constants.MAX_NUMBER_STREAMS

    def _config_offset(self, config_id: int, what: str) -> int:
        assert self.config.has_config(config_id), (
            f"The design does not expose a {what}"
        )
        return self.config.get_config_bounds(config_id)[0]

    def _heap_base_address(self, decoder: int) -> int:
        """The buffer the heap writer of `decoder` will fill.

        Registering the heap expectation allocates it and pushes the handle to the FPGA, so
        this is only meaningful afterwards. Only the first allocation is used: the decoder
        addresses its heap linearly, so a payload spilling into a follow-up allocation would
        not be contiguous. Every workload here stays well inside one allocation.
        """
        allocations = self.memory_manager.allocations.get(heap_stream(decoder), {})
        if len(allocations) == 0:
            return 0  # Nothing in the heap, e.g., because all strings are inline
        assert len(allocations) == 1, (
            f"Heap of decoder {decoder} spilled into {len(allocations)} allocations; "
            "the decoder assumes one contiguous heap buffer per column chunk."
        )
        return next(iter(allocations))

    def _stage_input(self, chunk_bytes: bytearray, misalign: int) -> int:
        """Writes a column chunk into simulated host memory and returns its vaddr.

        ReadReqGenerator rounds the request address *down* to DATABEAT_SIZE and lengthens the
        request to match, and LocalRead masks the leading `head` bytes back off. The
        rounded-down address still has to be backed by memory, so the allocation is padded on
        both sides and the chunk placed at a known offset inside it -- the simulator's
        allocator packs allocations tightly and would otherwise hand out an address whose
        aligned-down form falls into the previous allocation's gap.
        """
        data = bytearray(chunk_bytes)
        io = self.get_io_writer()
        base = io.allocate_next_free_sim_memory(len(data) + 2 * DATABEAT_SIZE)
        vaddr = -(-base // DATABEAT_SIZE) * DATABEAT_SIZE + misalign
        io.write_to_sim_memory(vaddr, data)
        return vaddr

    def _enqueue_read(
        self, read_req_offset: int, stream: int, chunk_bytes: bytearray, misalign: int
    ):
        """Stages a column chunk in host memory and points LocalRead at it."""
        vaddr = self._stage_input(chunk_bytes, misalign)
        base = read_req_offset + READ_REQ_REGS_PER_STREAM * stream
        self.write_register(fpga_register.vFPGARegister(base + 0, _register_value(vaddr)))
        self.write_register(
            fpga_register.vFPGARegister(base + 1, _register_value(len(chunk_bytes)))
        )
        # base + 2 is the Coyote thread id, which LocalRead hardwires to 0.

    def _prepare_read_requests(self, read_req_offset: int) -> None:
        """Transport-specific setup before any chunk requests are enqueued."""

    def run_jobs(self, jobs: "dict[int, list[_Job]]", misalign: int = 0):
        """Drives each decoder with its list of chunks and asserts every output.

        At most one string chunk per decoder: the heap buffer for a second one is only handed
        out after the first one's completion interrupt, so its addresses cannot be predicted
        while building the expectation.

        `misalign` offsets every staged chunk from a DATABEAT_SIZE boundary, exercising
        LocalRead's head stripping.
        """
        assert 0 <= misalign < DATABEAT_SIZE
        for decoder, decoder_jobs in jobs.items():
            assert decoder < self.num_decoders
            assert sum(job.is_string for job in decoder_jobs) <= 1, (
                f"decoder {decoder}: only one string chunk per run is supported"
            )

        self.jobs = jobs
        self.misalign = misalign
        self.simulate_fpga()
        self.assert_simulation_output()

    def configure(self):
        config = self.config.get_config(ColumnChunkDecoderConfig)
        read_req_offset = self._config_offset(READ_REQ_CONFIG_ID, "ReadReqConfig")
        self._prepare_read_requests(read_req_offset)

        # Heap expectations first. They are address-independent, and registering them is what
        # allocates the buffers whose addresses the string chunks need.
        for decoder, decoder_jobs in self.jobs.items():
            for job in decoder_jobs:
                if job.is_string:
                    self.set_expected_output(
                        heap_stream(decoder),
                        fpga_stream.Stream(
                            fpga_stream.StreamType.UNSIGNED_INT_8, list(job.heap_bytes)
                        ),
                    )

        for decoder, decoder_jobs in self.jobs.items():
            for job in decoder_jobs:
                heap_addr = self._heap_base_address(decoder) if job.is_string else 0
                chunk, values = job.finish(heap_addr)

                # German records are compared as raw bytes; everything else at the chunk's
                # own width.
                if chunk.is_string:
                    expected = fpga_stream.Stream(
                        fpga_stream.StreamType.UNSIGNED_INT_8, values
                    )
                else:
                    expected = fpga_stream.Stream(chunk.output_type(), values)
                self.set_expected_output(values_stream(decoder), expected)

                compression = Compression.SNAPPY if chunk.compression else Compression.RAW
                config.enqueue_column_chunk(
                    decoder, compression, chunk.num_values, chunk.data_type, chunk.is_required,
                    chunk.logical_type if not chunk.is_string else None, chunk.heap_addr
                )

                self._enqueue_read(
                    read_req_offset, decoder, chunk.chunk_bytes, self.misalign
                )


    # -- One decoder --------------------------------------------------------

    def test_single_decoder_plain(self):
        """The narrowest end-to-end path: one chunk, one writer, no heap."""
        self.run_jobs({0: [plain_job(_PLAIN_OUTPUT)]})

    def test_single_decoder_hybrid(self):
        """A dictionary-encoded parquet chunk, so the SNAPPY and hybrid paths run."""
        self.run_jobs({0: [parquet_job("rle_data.parquet", _RLE_OUTPUT)]})

    def test_single_decoder_strings(self):
        """Both writers of one pair active - the arbiter has to interleave them on
        channel 0 without either transfer overtaking its own request."""
        self.run_jobs({0: [string_job([b"alpha", b"b" * 30, b"gamma"])]})

    def test_single_decoder_all_inline_strings(self):
        """Every string fits inline, so the heap carries the raw page bytes but no record
        refers into it."""
        self.run_jobs({0: [string_job([b"a", b"bb", b"ccc", b"abcdefghijkl"])]})

    def test_single_decoder_sequential_chunks(self):
        """Several chunks back to back on one decoder: one read request and one
        acquire/transfer cycle per chunk, each with its own buffer."""
        self.run_jobs(
            {
                0: [
                    plain_job(_PLAIN_OUTPUT),
                    parquet_job("rle_data.parquet", _RLE_OUTPUT),
                    plain_job(
                        list(range(-100, 100)), fpga_stream.StreamType.SIGNED_INT_32
                    ),
                ]
            }
        )

    def test_single_decoder_empty_strings(self):
        """All strings empty, so the heap is nothing but length prefixes.

        This is the degenerate case for the scheduler's flow accounting, which counts one
        completion per enqueued buffer and concludes a flow at zero: it assumes a string chunk
        always raises exactly one heap interrupt. A chunk whose heap payload is entirely
        length prefixes is the closest the decoder gets to writing nothing at all -- if the
        writer stayed silent here, the flow would never complete.
        """
        self.run_jobs({0: [string_job([b"", b"", b""])]})

    def test_single_decoder_unaligned_input(self):
        """The chunk does not start on a DATABEAT_SIZE boundary, so ReadReqGenerator rounds
        the request down and LocalRead has to strip the leading bytes back off."""
        self.run_jobs({0: [string_job([b"unaligned", b"u" * 30])]}, misalign=13)

    # -- Several decoders ---------------------------------------------------

    def test_all_decoders_plain(self):
        """One chunk per decoder at once: cross-channel request arbitration and completion
        demultiplexing, with only one writer live per channel."""
        self.run_jobs(
            {d: [plain_job(list(range(0, 200 + d)))] for d in range(self.num_decoders)}
        )

    def test_all_decoders_strings(self):
        """Every logical stream live at once - the heaviest load the pair arbiters and the
        shared request/completion paths can see."""
        self.run_jobs(
            {
                d: [
                    string_job(
                        [bytes([0x41 + d]) * (10 + 5 * d), b"s%d" % d, b"z" * (20 + d)]
                    )
                ]
                for d in range(self.num_decoders)
            }
        )

    def test_mixed_string_and_fixed_decoders(self):
        """Heap-bearing and heap-silent decoders side by side. The fixed decoder must raise
        no interrupt at all on its heap stream, so a spurious empty transfer there shows up
        as an unexpected completion."""
        self.run_jobs(
            {
                0: [string_job([b"mixed", b"m" * 25])],
                1: [plain_job(_PLAIN_OUTPUT)],
            }
        )

    def test_interleaved_string_and_fixed_on_one_decoder(self):
        """The config-FIFO pairing: the heap register is written for the string chunk only,
        so a fixed chunk queued behind it must not consume the string's heap address."""
        self.run_jobs(
            {
                0: [
                    plain_job(_PLAIN_OUTPUT),
                    string_job([b"after-fixed", b"q" * 24]),
                    plain_job(
                        list(range(0, 100)), fpga_stream.StreamType.SIGNED_INT_32
                    ),
                ]
            }
        )

    def test_dictionary_flush(self):
        """A chunk whose trailing plain page forces the dictionary flush, running against
        unrelated string traffic on the other channel."""
        tricky = make_tricky("rle_data_rg0_col0", len(_RLE_OUTPUT), _PLAIN_OUTPUT, 2)
        self.run_jobs(
            {
                0: [fixed_job(tricky, _RLE_OUTPUT * 2 + _PLAIN_OUTPUT)],
                1: [string_job([b"c" * 40, b"dd"])],
            }
        )

    # -- Output sizing ------------------------------------------------------

    def test_large_output_multiple_allocations(self):
        """Output larger than one allocation, so the writer takes several buffers and the
        manager sees non-last interrupts before the final one."""
        self.overwrite_memory_manager(allocation_size=1024, transfer_size=512)
        self.run_jobs({0: [plain_job(list(range(0, 1000)))]})

    def test_small_transfers(self):
        """One data beat per transfer, maximising request/completion traffic through the
        shared channel."""
        self.overwrite_memory_manager(allocation_size=512, transfer_size=64)
        self.run_jobs({0: [string_job([b"tiny", b"t" * 40])]})

    def overwrite_memory_manager(self, allocation_size: int, transfer_size: int):
        """Resizes the FPGA-initiated transfers, mirroring output_writer_test.py."""
        self.memory_manager = memory_manager.FPGAOutputMemoryManager(
            self.get_io_writer(),
            self.config,
            allocation_size,
            transfer_size,
            all_done_callback=self.memory_manager_all_done_callback_spawner,
        )
        self.set_system_verilog_defines(
            {"TRANSFER_SIZE_BYTES_OVERWRITE": str(transfer_size)}
        )
