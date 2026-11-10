#include "oasis/operator.hpp"

#include "oasis/configuration.hpp"
#include "oasis/oasis_context.hpp"
#include "parcore/configuration.hpp"

#include <cassert>
#include <cstdint>
#include <stdexcept>

namespace oasis {

void RDMASourceOperator::apply(libstf::stream_t stream, OasisContext &ctx) {
    ctx.config<ReadReqConfig>()->enqueue_read(stream, offset_, size_);
}

void RDMASourceOperator::print(std::ostream &os) const {
    os << "RDMASource(offset=" << offset_ << ", size=" << size_ << ")";
}

void LocalSourceOperator::apply(libstf::stream_t stream, OasisContext &ctx) {
    if (ctx.isRDMAEnabled()) {
        throw std::runtime_error("LocalSourceOperator cannot be used when RDMA is enabled");
    }

    // Coyote needs a 64B-aligned source address or it emits databeats with a broken keep signal.
    const auto &buffer = input_buffer_;
    assert((reinterpret_cast<uintptr_t>(buffer->ptr) % 64) == 0);
    ctx.tlb_manager()->ensure_tlb_mapping(buffer->ptr, buffer->capacity);

    ctx.config<ReadReqConfig>()->enqueue_read(stream, reinterpret_cast<uintptr_t>(buffer->ptr),
                                              buffer->size);
}

void LocalSourceOperator::print(std::ostream &os) const {
    os << "LocalSource(ptr=" << input_buffer_->ptr << ", size=" << input_buffer_->size << ")";
}

void DecodeColumnChunkOperator::apply(libstf::stream_t stream, OasisContext &ctx) {
    auto config = ctx.config<parcore::ColumnChunkDecoderConfig>();
    // ParCore takes defer_dict_decoding before the heap address: both are defaulted, and passing
    // the address in its place would silently request deferred decoding with heap address 0.
    config->enqueue_column_chunk(stream, compression_, num_values_, type_, is_required_,
                                 logical_type_, defer_dict_decoding_, string_heap_address_);
}

void DecodeColumnChunkOperator::print(std::ostream &os) const {
    os << "DecodeColumnChunk(compression=" << compression_ << ", num_values=" << num_values_
       << ", type=" << type_ << ", is_required=" << is_required_
       << ", logical_type=" << logical_type_;
    if (string_heap_address_ != 0) {
        os << ", heap=0x" << std::hex << string_heap_address_ << std::dec;
    }
    if (defer_dict_decoding_) {
        os << ", defer_dict_decoding";
    }
    os << ")";
}

void LocalSinkOperator::apply(libstf::stream_t channel, OasisContext &ctx) {
    for (const auto &buffer : buffers_) {
        ctx.enqueue_output_buffer(values_stream(channel), *buffer);
    }
    if (heap_buffer_) {
        ctx.enqueue_output_buffer(heap_stream(channel), *heap_buffer_);
    }
}

void LocalSinkOperator::print(std::ostream &os) const {
    size_t bytes = 0;
    for (const auto &buffer : buffers_) {
        bytes += buffer->size;
    }
    os << "HostBufferSink(tag=" << tag_ << ", buffers=" << buffers_.size() << ", size=" << bytes;
    if (heap_buffer_) {
        os << ", heap=" << heap_buffer_->size;
    }
    os << ")";
}

} // namespace oasis
