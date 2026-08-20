"""RDMA end-to-end tests for the shipping OASIS vFPGA top.

This runs the same workload matrix as vfpga_top_test, but configures the
shipping top with RDMA enabled and stages each Parquet column chunk in the
simulator's remote-memory model. The FPGA therefore fetches every chunk through
RDMARead and axis_rreq_recv before the shared decoder and paired-output writer
paths are exercised.

Build this project separately from the local simulation:

    scripts/setup_simulation.sh --rdma on --decoders 3 --dir build-sim-rdma

Three decoders plus the RDMA bypass channel make four physical channels and
eight logical output streams, exactly filling the current three-bit interrupt
stream-id space. Four physical channels also satisfy libstf_utils' requirement
that the simulator stream count be even.
"""

from coyote_test import constants, fpga_register

import vfpga_top_test


class VFPGATopRDMATestCase(vfpga_top_test.VFPGATopTestCase):
    """The local suite with only its input transport replaced by RDMA."""

    # In RDMA mode the last physical channel is the decoder-less bypass path.
    num_decoders = constants.MAX_NUMBER_STREAMS - 1

    def setUp(self):
        super().setUp()
        # The simulated RDMA server has a separate address space, so choose stable,
        # non-overlapping addresses without consuming host-output allocations.
        self._next_remote_vaddr = vfpga_top_test.DATABEAT_SIZE

    def _prepare_read_requests(self, read_req_offset: int) -> None:
        # Production creates one queue pair per physical channel. The simulator
        # routes responses by request destination, while carrying ctid as the AXI id;
        # distinct values keep the test faithful to the host setup.
        for stream in range(constants.MAX_NUMBER_STREAMS):
            base = read_req_offset + vfpga_top_test.READ_REQ_REGS_PER_STREAM * stream
            self.write_register(
                fpga_register.vFPGARegister(
                    base + 2, vfpga_top_test._register_value(stream)
                )
            )

    def _stage_remote_input(self, chunk_bytes: bytearray, misalign: int) -> int:
        """Place one chunk in remote memory and return its RDMA offset.

        RDMARead rounds the request length up to a full databeat. The remote
        allocation must include that tail or the simulator correctly rejects the
        request as extending beyond its memory segment. DataRewriteLast strips
        these padding bytes again before the decoder.
        """
        data = bytearray(chunk_bytes)
        beat = vfpga_top_test.DATABEAT_SIZE
        padded_size = ((max(len(data), 1) + beat - 1) // beat) * beat
        vaddr = ((self._next_remote_vaddr + beat - 1) // beat) * beat + misalign
        padded = data + bytearray(padded_size - len(data))
        self.get_io_writer().remote_rdma_write(vaddr, padded)
        self._next_remote_vaddr = vaddr + padded_size + beat
        return vaddr

    def _enqueue_read(
        self, read_req_offset: int, stream: int, chunk_bytes: bytearray, misalign: int
    ):
        vaddr = self._stage_remote_input(chunk_bytes, misalign)
        base = read_req_offset + vfpga_top_test.READ_REQ_REGS_PER_STREAM * stream
        self.write_register(
            fpga_register.vFPGARegister(
                base + 0, vfpga_top_test._register_value(vaddr)
            )
        )
        self.write_register(
            fpga_register.vFPGARegister(
                base + 1, vfpga_top_test._register_value(len(chunk_bytes))
            )
        )
