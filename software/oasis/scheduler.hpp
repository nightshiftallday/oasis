#pragma once

#include "oasis/configuration.hpp"
#include "oasis/query_splinter.hpp"
#include "oasis/splinter_result.hpp"

#include <libstf/common.hpp>

#include <array>
#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <list>
#include <memory>
#include <mutex>
#include <optional>
#include <thread>
#include <vector>

namespace oasis {

class OasisContext;

/**
 * Describes one hardware stream the scheduler dispatches onto: the capability it provides and the
 * hardware bounds of its pipeline. For now OasisContext hardcodes the descriptions from the
 * synthesized configuration; eventually they could be auto-discovered from the hardware.
 */
struct StreamDescription {
    StreamCapability capability = StreamCapability::DECODE;
    // Maximum in-flight flows (the hardware request/config FIFO depth). For DECODE streams this
    // seeds the runtime-tunable pipeline depth.
    size_t max_flows = 1;
    // Maximum in-flight output buffers (the hardware output-writer FIFO depth).
    size_t max_buffers = 1;
};

/**
 * Bridges unbounded query concurrency to the fixed stream count. Callers submit QuerySplinters and
 * get a future in return. The scheduler owns the hardware streams, each described by the
 * capability it provides and its pipeline bounds (see StreamDescription).
 *
 * Flows are capability-matched to streams (see StreamCapability): decode flows are load-balanced
 * across the DECODE streams, raw source->sink flows can only run on a BYPASS stream. Each
 * capability has its own dispatch queue so a capability whose streams are full never head-of-line
 * blocks the other.
 *
 * Two knobs are tunable at runtime and apply to the DECODE streams: the number of *active* streams
 * (the span the dispatcher load-balances across) and the per-stream pipeline depth. Streams of
 * other capabilities are always active, with depths fixed by their hardware FIFO bounds
 * (max_flows/max_buffers).
 */
class Scheduler {
  public:
    // `streams` describes all available hardware streams, indexed by hardware stream id.
    Scheduler(OasisContext &ctx, std::vector<StreamDescription> streams);
    ~Scheduler();

    Scheduler(const Scheduler &)            = delete;
    Scheduler &operator=(const Scheduler &) = delete;

    // Expands the splinter into its flows, determines each flow's required capability (see
    // StreamCapability), and pushes each onto its capability's unbounded queue, returning instantly
    // with a future. The splinter completes (its single result channel closes) only once every flow
    // has drained. Throws if a flow requires a capability no stream provides, or could never fit
    // any providing stream's bounds.
    SplinterResultHandle submit(QuerySplinter splinter);

    [[nodiscard]] libstf::stream_t num_streams() const { return num_streams_; }
    [[nodiscard]] libstf::stream_t num_decode_streams() const { return num_decode_streams_; }

    void                           set_active_streams(libstf::stream_t active);
    [[nodiscard]] libstf::stream_t active_streams() const { return active_streams_.load(); }

    void                 set_pipeline_depth(size_t depth);
    [[nodiscard]] size_t pipeline_depth() const { return queue_depth_.load(); }

    // Flows waiting in the dispatcher queues (not yet enqueued to the hardware).
    [[nodiscard]] size_t queued_flows() const;

    // Flows currently enqueued across all streams (enqueued in hardware, awaiting completion).
    [[nodiscard]] size_t in_flight_flows() const;

    // Invoked from the interrupt switch for a stream interrupt. The hardware interrupts once per
    // output buffer. `stream` is a *logical* output stream (see configuration.hpp): each channel has
    // two, values and string heap, with independent hardware FIFOs. This pops the front pending
    // completion for that logical stream and pushes the corresponding buffer onto the splinter's
    // result channel. A flow concludes once every buffer it enqueued -- across both of its logical
    // streams -- has been accounted for.
    void handle_completion(libstf::stream_t stream, uint32_t bytes_written, bool last);

  private:
    struct SplinterCompletion {
        std::shared_ptr<SplinterResultChannel> channel;
        std::atomic<size_t>                    outstanding_flows;
    };

    // One in-flight flow on a channel. It owns the flow's operators (including the sink's output
    // buffers) until reaped, plus a shared pointer to the splinter's completion record.
    //
    // `outstanding_buffers` is what concludes the flow. A string flow enqueues buffers on several of
    // its channel's logical streams, which complete independently and in either order, so no single
    // interrupt can be "the last one" -- the flow is done when this reaches zero. It is written by
    // the dispatcher before the flow is enqueued to the hardware and decremented by the interrupt
    // thread, so it is atomic; everything else here is guarded by the channel mutex.
    struct InFlight {
        OperatorFlow                        flow;
        std::shared_ptr<SplinterCompletion> completion;
        std::atomic<size_t>                 outstanding_buffers{0};
        bool                                done = false;
    };

    // One enqueued sink output buffer awaiting its hardware interrupt, recorded in its logical
    // stream's FIFO in enqueue order. Holds everything handle_completion needs without re-locking
    // the in-flight list: the buffer to surface, the tag, which logical stream it belongs to, the
    // splinter completion, a stable iterator to the owning in-flight slot, and whether this is the
    // final buffer *of its own logical stream* (which is what the hardware's `last` flag marks).
    struct PendingCompletion {
        std::shared_ptr<libstf::Buffer>     buffer;
        size_t                              tag  = 0;
        OutputSlot                          slot = OutputSlot::VALUES;
        std::shared_ptr<SplinterCompletion> completion;
        std::list<InFlight>::iterator       in_flight;
        bool                                last = false;
    };

    // A queued flow waiting for the dispatcher to place it on a channel providing its capability.
    // Carries the shared completion record of the splinter it belongs to and its sink's buffer count
    // per logical stream (the buffer-slot dispatch gate needs them before the flow is placed; the
    // two streams have independent hardware FIFOs, so they are gated independently).
    struct Pending {
        OperatorFlow                        flow;
        std::shared_ptr<SplinterCompletion> completion;
        StreamCapability                    capability = StreamCapability::DECODE;
        std::array<size_t, OUTPUT_STREAMS_PER_CHANNEL> num_buffers{};

        [[nodiscard]] size_t total_buffers() const {
            size_t total = 0;
            for (size_t n : num_buffers) {
                total += n;
            }
            return total;
        }
    };

    // Per-channel pipeline: The channel's bounds (from its StreamDescription), the list of in-flight
    // flows, and the count of flows/buffers currently enqueued on it. `enqueued` is the
    // load-balancing metric and the flow FIFO gate. `enqueued_buffers` gates the OutputWriter FIFO,
    // per logical stream -- values and heap have separate hardware FIFOs, so a shared counter would
    // be both wrong and needlessly conservative. All are atomic and held under no lock: the
    // dispatcher bumps them when it places a flow and the interrupt thread decrements them as
    // completions land. Keeping them lock-free is what breaks the lock cycle -- handle_completion
    // must never take dispatch_mutex_ while the dispatcher holds dispatch_mutex_ and is waiting for
    // the channel mutex. `in_flight`/`done` are guarded by the channel mutex.
    //
    // `completions` is per logical stream: the streams complete independently and in any order,
    // so one shared FIFO would mismatch a heap interrupt against a values buffer.
    struct StreamState {
        std::mutex          mutex;
        std::list<InFlight> in_flight;
        // Enqueued buffers awaiting interrupts, in order, per logical stream.
        std::array<std::deque<PendingCompletion>, OUTPUT_STREAMS_PER_CHANNEL> completions;
        std::atomic<size_t>                                                   enqueued{0};
        std::array<std::atomic<size_t>, OUTPUT_STREAMS_PER_CHANNEL>           enqueued_buffers{};
        size_t                                                                max_flows   = 1;
        size_t                                                                max_buffers = 1;
    };

    OasisContext                 &ctx_;
    const libstf::stream_t        num_streams_;
    const libstf::stream_t        num_decode_streams_; // streams providing DECODE (the tunable span)
    std::atomic<libstf::stream_t> active_streams_;
    std::atomic<size_t>           queue_depth_;

    std::vector<std::unique_ptr<StreamState>> streams_;
    // Hardware stream ids providing each capability, in id order. Indexed by StreamCapability.
    std::array<std::vector<libstf::stream_t>, NUM_STREAM_CAPABILITIES> streams_by_capability_;

    mutable std::mutex      dispatch_mutex_;
    std::condition_variable dispatch_cv_;
    // One queue per capability, so full streams of one capability never block dispatch to the
    // other. Indexed by StreamCapability.
    std::array<std::deque<Pending>, NUM_STREAM_CAPABILITIES> queues_;
    bool                    stop_ = false;
    std::thread             dispatcher_;

    // The dispatcher loop: Pops a queue head whose capability has a stream with a free slot, and
    // dispatches there. Parks on dispatch_cv_ when no queued flow is placeable. Reaps finished
    // slots before measuring load.
    void dispatch_loop();

    // Returns the least-loaded channel providing `capability` that can accept a flow whose sink
    // spans `num_buffers[slot]` buffers on each of its logical streams (its flow gate and both
    // buffer gates have room). For DECODE the candidates are the active span and the flow gate is
    // the runtime pipeline depth; for other capabilities every providing channel is a candidate,
    // gated by its hardware bounds. nullopt if none. Must be called holding dispatch_mutex_.
    std::optional<libstf::stream_t>
    pick_stream(StreamCapability                                      capability,
                const std::array<size_t, OUTPUT_STREAMS_PER_CHANNEL> &num_buffers) const;

    // Applies a flow on `channel`: Records a pending completion per sink buffer, in its logical
    // stream's FIFO, and enqueues the buffers to the FPGA, parking the flow in the channel's
    // in-flight list. Called only by the dispatcher, with a slot already reserved
    // (enqueued/enqueued_buffers atomically bumped).
    void dispatch_to(libstf::stream_t channel, Pending &pending);

    // Splices `done` in-flight slots into `finished` (without destroying them, so the caller can
    // destroy them outside the locks -- keeping flow/buffer teardown off the scheduler locks).
    // Must be called holding ss.mutex.
    void reap(StreamState &ss, std::list<InFlight> &finished);
};

} // namespace oasis
