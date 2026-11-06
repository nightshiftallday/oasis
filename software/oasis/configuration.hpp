#pragma once

#include "libstf/common.hpp"
#include <coyote/cThread.hpp>
#include <libstf/configuration.hpp>
#include <parcore/configuration.hpp>

#include <vector>

namespace oasis {

constexpr const uint64_t OASIS_SYSTEM_ID = 0x0A515;

/**
 * Logical output streams per physical channel. The hardware's PairedOutputWriter puts a decoder's
 * values, its string heap and its string dictionary on the same Coyote channel but gives them
 * separate MemConfig slots and separate interrupt stream ids, numbered 3*channel, 3*channel + 1 and
 * 3*channel + 2. Everything the host addresses per output buffer -- MemConfig::enqueue_buffer, the
 * interrupt's stream id -- uses that logical number; everything that addresses a decoder --
 * ColumnChunkDecoderConfig, ReadReqConfig -- uses the channel.
 */
constexpr size_t OUTPUT_STREAMS_PER_CHANNEL = 3;

// Which of a channel's logical output streams a buffer belongs to. DICT only carries data for a
// dictionary encoded string chunk: the german_str_t records of its dictionary page, while VALUES
// then holds one uint32_t dictionary index per row (a DuckDB dictionary vector).
enum class OutputSlot : size_t { VALUES = 0, HEAP = 1, DICT = 2 };

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

constexpr libstf::stream_t dict_stream(libstf::stream_t channel) {
    return logical_stream(channel, OutputSlot::DICT);
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

// GenericConfig read side: local address 0 is the config ID, 1 the OutputWriter notify counter,
// and (RDMA builds only) 2..6 the bypass stream's StreamProfiler counters.
constexpr const uint64_t GENERIC_CONFIG_ID                 = static_cast<uint64_t>(-1);
constexpr const uint32_t GENERIC_CONFIG_NOTIFY_COUNT_REG   = 1;
constexpr const uint32_t GENERIC_CONFIG_BYPASS_PROFILE_REG = 2;
constexpr const uint32_t NUM_BYPASS_PROFILE_REGS           = 5;

class GenericConfig : public libstf::Config {
  public:
    GenericConfig(std::shared_ptr<coyote::cThread> cthread, uint32_t addr_offset,
                  uint32_t num_regs);

    uint64_t notify_count();

    bool has_bypass_profile() const;

    parcore::StreamProfile read_bypass_profile();

    static constexpr uint64_t ID = GENERIC_CONFIG_ID;
};

} // namespace oasis
