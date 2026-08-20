#pragma once

#include <libstf/buffer.hpp>
#include <libstf/common.hpp>
#include <parcore/metadata/metadata.hpp>

#include <cstdint>
#include <limits>
#include <memory>
#include <ostream>
#include <stdexcept>
#include <vector>

namespace oasis {

class OasisContext;

/**
 * Operator as part of a QuerySplinter, bound to a stream at schedule time. apply() has to run
 * under a lock on the respective stream and configures the hardware components. Single-use.
 */
class Operator {
  public:
    virtual ~Operator() = default;

    virtual void apply(libstf::stream_t stream, OasisContext &ctx) = 0;

    virtual void print(std::ostream &os) const = 0;
};

inline std::ostream &operator<<(std::ostream &os, const Operator &op) {
    op.print(os);
    return os;
}

/**
 * Operator that fetches data into a stream. Hides the transport (e.g., RDMA or local DMA).
 */
class SourceOperator : public Operator {};

/**
 * Triggers a remote RDMA read. The hardware pulls bytes straight into the stream, so no host
 * buffer. Offsets are relative to the remote RDMA region base (see ReadReqConfig::enqueue_read).
 */
class RDMASourceOperator final : public SourceOperator {
  public:
    RDMASourceOperator(uint64_t offset, size_t size) : offset_(offset), size_(size) {
        if (size > std::numeric_limits<uint32_t>::max()) {
            throw std::runtime_error("RDMA read size exceeds 32-bit limit");
        }
    }

    void apply(libstf::stream_t stream, OasisContext &ctx) override;
    void print(std::ostream &os) const override;

  private:
    uint64_t offset_;
    size_t   size_;
};

/**
 * Triggers a local read of of the input host buffer into the stream. Owns the input buffer for the
 * splinter's lifetime so it stays mapped until the hardware has consumed it. Throws if RDMA is 
 * enabled, since only one of the data paths is synthesized into the hardware at a time
 */
class LocalSourceOperator final : public SourceOperator {
  public:
    explicit LocalSourceOperator(std::shared_ptr<libstf::Buffer> input_buffer)
        : input_buffer_(std::move(input_buffer)) {}

    void apply(libstf::stream_t stream, OasisContext &ctx) override;
    void print(std::ostream &os) const override;

  private:
    std::shared_ptr<libstf::Buffer> input_buffer_;
};

/**
 * Programs the configuration (compression, value count, type) via
 * ColumnChunkDecoderConfig::enqueue_column_chunk.
 *
 * For GERMAN_STR_T the decoder also needs the base address of the buffer it will write the string
 * heap into, because the addresses it embeds in the german_str_t records are absolute. That address
 * comes from a second config register the hardware pops *only* for GERMAN_STR_T, so it must be
 * supplied exactly for string chunks and never otherwise -- one stray write and the two config
 * FIFOs are permanently out of step. The constructor enforces the iff.
 */
class DecodeColumnChunkOperator final : public Operator {
  public:
    DecodeColumnChunkOperator(parcore::metadata::Compression compression, uint64_t num_values,
                              libstf::type_t type, uint64_t string_heap_address = 0)
        : compression_(compression), num_values_(num_values), type_(type),
          string_heap_address_(string_heap_address) {
        const bool is_string = type_ == libstf::type_t::GERMAN_STR_T;
        if (is_string != (string_heap_address_ != 0)) {
            throw std::runtime_error(
                is_string ? "DecodeColumnChunkOperator: a GERMAN_STR_T chunk needs a string heap "
                            "address"
                          : "DecodeColumnChunkOperator: a string heap address was given for a "
                            "non-string chunk, which would desynchronise the config FIFOs");
        }
    }

    void apply(libstf::stream_t stream, OasisContext &ctx) override;
    void print(std::ostream &os) const override;

  private:
    parcore::metadata::Compression compression_;
    uint64_t                       num_values_;
    libstf::type_t                 type_;
    uint64_t                       string_heap_address_;
};

/**
 * Sink that writes the flow's output to host buffers pre-allocated by the caller at the exact
 * transfer size. A flow ends in exactly one sink; a transfer larger than a single output-writer
 * buffer spans multiple buffers, which the hardware fills in enqueue order.
 *
 * A sink covers *both* of its channel's logical output streams. The values buffers go to
 * values_stream(channel); a string chunk additionally has one heap buffer, which goes to
 * heap_stream(channel). apply() enqueues each to the right stream, in order.
 *
 * The heap is limited to a single buffer on purpose: the decoder walks it linearly from the base
 * address it was configured with, so a heap spilling into a second (non-adjacent) allocation would
 * produce records pointing outside it.
 */
class LocalSinkOperator final : public Operator {
  public:
    // `buffers` are the right-sized output buffers the hardware will write the decoded values into,
    // in write order (each capacity must be a multiple of BYTES_PER_FPGA_TRANSFER). `heap_buffer` is
    // the string heap destination for a GERMAN_STR_T flow, and must be null otherwise -- enqueueing
    // one for a non-string flow leaves it forever unwritten in the hardware FIFO, which desynchronises
    // every later heap transfer on that stream. `tag` identifies this flow's output to the consumer,
    // since a QuerySplinter's flows can return in any order.
    explicit LocalSinkOperator(std::vector<std::shared_ptr<libstf::Buffer>> buffers, size_t tag = 0,
                      std::shared_ptr<libstf::Buffer> heap_buffer = nullptr)
        : tag_(tag), buffers_(std::move(buffers)), heap_buffer_(std::move(heap_buffer)) {
        if (buffers_.empty()) {
            throw std::runtime_error("LocalSinkOperator requires at least one buffer");
        }
    }

    explicit LocalSinkOperator(std::shared_ptr<libstf::Buffer> buffer, size_t tag = 0,
                               std::shared_ptr<libstf::Buffer> heap_buffer = nullptr)
        : LocalSinkOperator(std::vector<std::shared_ptr<libstf::Buffer>>{std::move(buffer)}, tag,
                            std::move(heap_buffer)) {}

    void apply(libstf::stream_t stream, OasisContext &ctx) override;
    void print(std::ostream &os) const override;

    [[nodiscard]] const std::vector<std::shared_ptr<libstf::Buffer>> &buffers() const {
        return buffers_;
    }
    [[nodiscard]] const std::shared_ptr<libstf::Buffer> &heap_buffer() const { return heap_buffer_; }
    [[nodiscard]] bool   has_heap() const { return heap_buffer_ != nullptr; }
    [[nodiscard]] size_t tag() const { return tag_; }

  private:
    size_t                                       tag_;
    std::vector<std::shared_ptr<libstf::Buffer>> buffers_;
    std::shared_ptr<libstf::Buffer>              heap_buffer_;
};

} // namespace oasis
