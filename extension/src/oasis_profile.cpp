#include "oasis_profile.hpp"

#include "oasis_context_cache_entry.hpp"
#include "oasis/configuration.hpp"
#include "oasis/oasis_context.hpp"
#include "parcore/configuration.hpp"

#include "duckdb/function/table_function.hpp"

namespace duckdb {

namespace {

// Hardware facts the throughput derivation is based on: every stream handshake transports one
// 64-byte databeat, and the accelerator runs at 250 MHz (a 4 ns clock period).
constexpr double BYTES_PER_HANDSHAKE = 64.0;
constexpr double CLOCK_PERIOD_NS = 4.0;

// The profiled streams of a decoder slot, in column order. "out" is the values stream, which carries
// the dictionary indices of a dictionary encoded string chunk.
constexpr const char *STREAM_NAMES[] = {"in", "out", "heap", "dict"};
constexpr idx_t NUM_PROFILED_STREAMS = sizeof(STREAM_NAMES) / sizeof(STREAM_NAMES[0]);
static_assert(NUM_PROFILED_STREAMS == parcore::COLUMN_CHUNK_DECODER_PROFILED_STREAMS,
              "one column group per stream of parcore::DecoderProfile");

// One materialized output row: the raw counters for a stream slot plus the derived throughput
// numbers. Decoder slots have a profiler on their input and on each of their three outputs; the
// RDMA bypass slot has a single profiler on its output stream, so its other columns are NULL.
// throughput is in GB/s (== bytes per nanosecond).
struct ProfileRow {
	uint64_t stream;
	parcore::StreamProfile profiles[NUM_PROFILED_STREAMS];
	bool present[NUM_PROFILED_STREAMS];
	double throughput_gbps[NUM_PROFILED_STREAMS];           // over the whole profiled window (incl. idle)
	double throughput_excl_idle_gbps[NUM_PROFILED_STREAMS]; // excluding inter-stream idle cycles
};

// Bytes transported divided by the time spent over `cycles` clock cycles, in GB/s. Returns 0 when
// there is no time to divide by so an unused stream reads as zero rather than NaN.
double ThroughputGBps(uint64_t handshakes, uint64_t cycles) {
	if (cycles == 0) {
		return 0.0;
	}
	double bytes = static_cast<double>(handshakes) * BYTES_PER_HANDSHAKE;
	double time_ns = static_cast<double>(cycles) * CLOCK_PERIOD_NS;
	return bytes / time_ns;
}

void SetStream(ProfileRow &row, idx_t s, const parcore::StreamProfile &p) {
	row.profiles[s] = p;
	row.present[s] = true;
	// Overall: every cycle the profiler observed for this stream, including idle gaps between
	// streams. Excluding-idle: the cycles the stream was actually running.
	uint64_t busy = p.handshakes_cycles + p.starved_cycles + p.stalled_cycles;
	row.throughput_gbps[s] = ThroughputGBps(p.handshakes_cycles, busy + p.idle_cycles);
	row.throughput_excl_idle_gbps[s] = ThroughputGBps(p.handshakes_cycles, busy);
}

ProfileRow EmptyRow(uint64_t stream) {
	ProfileRow row {};
	row.stream = stream;
	return row;
}

ProfileRow MakeRow(uint64_t stream, const parcore::DecoderProfile &p) {
	ProfileRow row = EmptyRow(stream);
	SetStream(row, 0, p.in);
	SetStream(row, 1, p.out);
	SetStream(row, 2, p.heap);
	SetStream(row, 3, p.dict);
	return row;
}

// The bypass stream has one StreamProfiler on its output (the RDMA-read data feeding the output
// writer), so only the out-side of the row is populated.
ProfileRow MakeBypassRow(uint64_t stream, const parcore::StreamProfile &p) {
	ProfileRow row = EmptyRow(stream);
	SetStream(row, 1, p);
	return row;
}

struct OasisProfileBindData : public TableFunctionData {
	vector<Identifier> names;
	vector<LogicalType> types;
};

// Reads every decoder's counters from the hardware once at init and holds the rows to emit.
struct OasisProfileGlobalState : public GlobalTableFunctionState {
	vector<ProfileRow> rows;
	idx_t offset = 0;

	idx_t MaxThreads() const override {
		return 1;
	}
};

// Column layout, kept in one place so bind and scan stay in sync.
void DefineColumns(vector<Identifier> &names, vector<LogicalType> &types) {
	auto add = [&](const char *name, LogicalType type) {
		names.emplace_back(name);
		types.emplace_back(std::move(type));
	};

	add("stream", LogicalType::UBIGINT);

	for (auto name : STREAM_NAMES) {
		string prefix(name);
		add((prefix + "_handshakes_cycles").c_str(), LogicalType::UBIGINT);
		add((prefix + "_starved_cycles").c_str(), LogicalType::UBIGINT);
		add((prefix + "_stalled_cycles").c_str(), LogicalType::UBIGINT);
		add((prefix + "_idle_cycles").c_str(), LogicalType::UBIGINT);
		add((prefix + "_last_handshakes").c_str(), LogicalType::UBIGINT);
	}

	for (auto name : STREAM_NAMES) {
		string prefix(name);
		add((prefix + "_throughput_gbps").c_str(), LogicalType::DOUBLE);
		add((prefix + "_throughput_excl_idle_gbps").c_str(), LogicalType::DOUBLE);
	}
}

unique_ptr<FunctionData> OasisProfileBind(ClientContext &context, TableFunctionBindInput &input,
                                          vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto bind_data = make_uniq<OasisProfileBindData>();
	DefineColumns(names, return_types);
	bind_data->names = names;
	bind_data->types = return_types;
	return std::move(bind_data);
}

unique_ptr<GlobalTableFunctionState> OasisProfileInitGlobal(ClientContext &context,
                                                            TableFunctionInitInput &input) {
	auto gstate = make_uniq<OasisProfileGlobalState>();

	auto &ctx = GetOrCreateOasisContext(context);
	auto config = ctx.config<parcore::ColumnChunkDecoderConfig>();

	auto num_decoders = config->num_decoders();
	gstate->rows.reserve(num_decoders + 1);
	for (libstf::stream_t decoder = 0; decoder < num_decoders; decoder++) {
		gstate->rows.push_back(MakeRow(decoder, config->read_profile(decoder)));
	}

	// The RDMA bypass stream occupies the last stream slot (== num_decoders) and has its own
	// StreamProfiler behind the GenericConfig. Older bitstreams have no GenericConfig and non-RDMA
	// builds expose no bypass counters, so those simply emit the decoder rows only.
	if (ctx.has_config<oasis::GenericConfig>()) {
		auto generic = ctx.config<oasis::GenericConfig>();
		if (generic->has_bypass_profile()) {
			gstate->rows.push_back(MakeBypassRow(num_decoders, generic->read_bypass_profile()));
		}
	}

	return std::move(gstate);
}

void OasisProfileFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &gstate = data_p.global_state->Cast<OasisProfileGlobalState>();

	idx_t remaining = gstate.rows.size() - gstate.offset;
	idx_t count = MinValue<idx_t>(remaining, STANDARD_VECTOR_SIZE);
	if (count == 0) {
		output.SetChildCardinality(0);
		return;
	}

	for (idx_t i = 0; i < count; i++) {
		const auto &row = gstate.rows[gstate.offset + i];

		idx_t col = 0;
		output.data[col++].SetValue(i, Value::UBIGINT(row.stream));

		// Streams without a profiler (all but `out` of the bypass row) are NULL.
		for (idx_t s = 0; s < NUM_PROFILED_STREAMS; s++) {
			const auto &p = row.profiles[s];
			for (uint64_t v : {p.handshakes_cycles, p.starved_cycles, p.stalled_cycles, p.idle_cycles,
			                   p.last_handshakes}) {
				output.data[col++].SetValue(i, row.present[s] ? Value::UBIGINT(v) : Value(LogicalType::UBIGINT));
			}
		}
		for (idx_t s = 0; s < NUM_PROFILED_STREAMS; s++) {
			for (double v : {row.throughput_gbps[s], row.throughput_excl_idle_gbps[s]}) {
				output.data[col++].SetValue(i, row.present[s] ? Value::DOUBLE(v) : Value(LogicalType::DOUBLE));
			}
		}
	}

	gstate.offset += count;
	output.SetChildCardinality(count);
}

} // namespace

void RegisterOasisProfileFunction(ExtensionLoader &loader) {
	TableFunction profile_function("oasis_stream_profile", {}, OasisProfileFunction, OasisProfileBind,
	                               OasisProfileInitGlobal);
	loader.RegisterFunction(profile_function);
}

} // namespace duckdb
