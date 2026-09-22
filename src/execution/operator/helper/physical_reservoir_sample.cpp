#include "duckdb/execution/operator/helper/physical_reservoir_sample.hpp"
#include "duckdb/execution/reservoir_sample.hpp"
#include "duckdb/common/atomic.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//

class SampleGlobalSinkState : public GlobalSinkState {
public:
	explicit SampleGlobalSinkState(Allocator &allocator, SampleOptions &options) {
		if (options.is_percentage) {
			auto percentage = options.sample_size.GetValue<double>();
			if (percentage == 0) {
				return;
			}
			sample = make_uniq<ReservoirSamplePercentage>(allocator, percentage,
			                                              static_cast<int64_t>(options.seed.GetIndex()));
		} else {
			auto size = NumericCast<idx_t>(options.sample_size.GetValue<int64_t>());
			if (size == 0) {
				return;
			}
			sample = make_uniq<ReservoirSample>(allocator, size, static_cast<int64_t>(options.seed.GetIndex()));
		}
	}

	//! The lock for updating the global aggoregate state
	//! Also used to update the global sample when percentages are used
	mutex lock;
	//! The reservoir sample
	unique_ptr<BlockingSample> sample;
};

unique_ptr<GlobalSinkState> PhysicalReservoirSample::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<SampleGlobalSinkState>(Allocator::Get(context), *options);
}

SinkResultType PhysicalReservoirSample::Sink(ExecutionContext &context, DataChunk &chunk,
                                             OperatorSinkInput &input) const {
	auto &global_state = input.global_state.Cast<SampleGlobalSinkState>();
	// Percentage only has a global sample.
	lock_guard<mutex> glock(global_state.lock);
	if (!global_state.sample) {
		// always gather full thread percentage
		auto &allocator = Allocator::Get(context.client);
		if (options->is_percentage) {
			double percentage = options->sample_size.GetValue<double>();
			if (percentage == 0) {
				return SinkResultType::FINISHED;
			}
			global_state.sample = make_uniq<ReservoirSamplePercentage>(allocator, percentage,
			                                                           static_cast<int64_t>(options->seed.GetIndex()));
		} else {
			idx_t num_samples = options->sample_size.GetValue<idx_t>();
			if (num_samples == 0) {
				return SinkResultType::FINISHED;
			}
			global_state.sample =
			    make_uniq<ReservoirSample>(allocator, num_samples, static_cast<int64_t>(options->seed.GetIndex()));
		}
	}
	global_state.sample->AddToReservoir(chunk);
	return SinkResultType::NEED_MORE_INPUT;
}

SinkCombineResultType PhysicalReservoirSample::Combine(ExecutionContext &context,
                                                       OperatorSinkCombineInput &input) const {
	return SinkCombineResultType::FINISHED;
}

SinkFinalizeType PhysicalReservoirSample::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                                   OperatorSinkFinalizeInput &input) const {
	return SinkFinalizeType::READY;
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class SampleGlobalSourceState : public GlobalSourceState {
public:
	atomic<idx_t> total_rows {0};
	atomic<idx_t> rows_scanned {0};
	atomic<double> progress {0.0};
};

unique_ptr<GlobalSourceState> PhysicalReservoirSample::GetGlobalSourceState(ClientContext &context) const {
	return make_uniq<SampleGlobalSourceState>();
}

ProgressData PhysicalReservoirSample::GetProgress(ClientContext &context, GlobalSourceState &gstate) const {
	return ProgressData {gstate.Cast<SampleGlobalSourceState>().progress.load(), 1.0, false};
}

void PhysicalReservoirSample::SourceFinished(ClientContext &context, GlobalSourceState &gstate) const {
	gstate.Cast<SampleGlobalSourceState>().progress = 1.0;
}

SourceResultType PhysicalReservoirSample::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                          OperatorSourceInput &input) const {
	auto &sink = this->sink_state->Cast<SampleGlobalSinkState>();
	auto &state = input.global_state.Cast<SampleGlobalSourceState>();
	lock_guard<mutex> glock(sink.lock);
	if (!sink.sample) {
		state.progress = 1.0;
		return SourceResultType::FINISHED;
	}
	auto sample_chunk = sink.sample->GetChunk();

	if (!sample_chunk) {
		state.progress = 1.0;
		return SourceResultType::FINISHED;
	}
	if (state.total_rows == 0) {
		auto remaining = options->is_percentage ? sink.sample->Cast<ReservoirSamplePercentage>().GetActiveSampleCount()
		                                        : sink.sample->Cast<ReservoirSample>().GetActiveSampleCount();
		state.total_rows = sample_chunk->size() + remaining;
	}
	chunk.Move(*sample_chunk);
	state.rows_scanned += chunk.size();
	state.progress = double(state.rows_scanned.load()) / double(state.total_rows.load());

	return SourceResultType::HAVE_MORE_OUTPUT;
}

InsertionOrderPreservingMap<string> PhysicalReservoirSample::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	result["Sample Size"] = options->sample_size.ToString() + (options->is_percentage ? "%" : " rows");
	SetEstimatedCardinality(result, estimated_cardinality);
	return result;
}

} // namespace duckdb
