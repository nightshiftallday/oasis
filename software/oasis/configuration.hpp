#pragma once

#include "libstf/common.hpp"
#include <coyote/cThread.hpp>
#include <libstf/configuration.hpp>

#include <vector>

namespace oasis {

constexpr const uint64_t OASIS_SYSTEM_ID = 0x0A515;

/**
 * Logical output streams per physical channel. The hardware's PairedOutputWriter puts a decoder's
 * values and its string heap on the same Coyote channel but gives them separate MemConfig slots and
 * separate interrupt stream ids, numbered 2*channel and 2*channel + 1. Everything the host
 * addresses per output buffer -- MemConfig::enqueue_buffer, the interrupt's stream id -- uses that
 * logical number; everything that addresses a decoder -- ColumnChunkDecoderConfig, ReadReqConfig --
 * uses the channel.
 */
constexpr size_t OUTPUT_STREAMS_PER_CHANNEL = 2;

// Which of a channel's two logical output streams a buffer belongs to.
enum class OutputSlot : size_t { VALUES = 0, HEAP = 1 };

constexpr libstf::stream_t logical_stream(libstf::stream_t channel, OutputSlot slot) {
    return static_cast<libstf::stream_t>(channel * OUTPUT_STREAMS_PER_CHANNEL +
                                         static_cast<size_t>(slot));
}

constexpr libstf::stream_t values_stream(libstf::stream_t channel) {
    return logical_stream(channel, OutputSlot::VALUES);
}

constexpr libstf::stream_t heap_stream(libstf::stream_t channel) {
    return logical_stream(channel, OutputSlot::HEAP);
}

constexpr libstf::stream_t stream_channel(libstf::stream_t logical) {
    return static_cast<libstf::stream_t>(logical / OUTPUT_STREAMS_PER_CHANNEL);
}

constexpr OutputSlot stream_slot(libstf::stream_t logical) {
    return static_cast<OutputSlot>(logical % OUTPUT_STREAMS_PER_CHANNEL);
}

// Per-stream write registers: [0] vaddr, [1] size, [2] ctid.
constexpr const uint64_t READ_REQ_CONFIG_REGS = 3;
constexpr const uint64_t READ_REQ_CONFIG_ID   = 0x2f966a70f04c0e93;

/**
 * Configues a hardware read request module to fetch data.
 */
class ReadReqConfig : public libstf::Config {
  public:
    ReadReqConfig(std::shared_ptr<coyote::cThread> cthread, uint32_t addr_offset,
                   uint32_t num_regs);

    /**
     * For RDMA reads, sets the base vaddr of the remote region that `stream`'s read addresses are
     * relative to. Each stream has its own queue pair, so the remote region base can differ per
     * stream. It must be set before the first enqueue_read() on that stream.
     */
    void set_base_vaddr(libstf::stream_t stream, uintptr_t base_vaddr);

    /**
     * Set the Coyote thread id the hardware issues `stream`'s reads for. Must be set before the
     * first enqueue_read() on that stream.
     */
    void set_ctid(libstf::stream_t stream, uint32_t ctid);

    /**
     * Triggers a read request using the `RDMARead` or `LocalRead` module.
     *
     * Reads are relative to the base virtual address set via set_base_vaddr().
     */
    void enqueue_read(libstf::stream_t stream, size_t vaddr, size_t size);

    const libstf::stream_t num_streams() const;

    static constexpr size_t MAXIMUM_NUM_ENQUEUED_REQUESTS = 64;

    static constexpr uint64_t ID = READ_REQ_CONFIG_ID;

  private:
    libstf::stream_t       num_streams_;
    std::vector<uintptr_t> base_vaddrs_;
};

} // namespace oasis
