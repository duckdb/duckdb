#include "duckdb/execution/operator/helper/physical_reservoir_sample.hpp"
#include "duckdb/execution/reservoir_sample.hpp"

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
			sample = make_uniq<ReservoirSamplePercentage>(allocator, percentage, options.seed);
		} else {
			auto size = options.sample_size.GetValue<int64_t>();
			if (size == 0) {
				return;
			}
			sample = make_uniq<ReservoirSample>(allocator, size, options.seed);
		}
	}

	//! The lock for updating the global aggregate state
	mutex lock;
	//! The reservoir sample
	unique_ptr<BlockingSample> sample;
};

unique_ptr<GlobalSinkState> PhysicalReservoirSample::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<SampleGlobalSinkState>(Allocator::Get(context), *options);
}

SinkResultType PhysicalReservoirSample::Sink(ExecutionContext &context, DataChunk &chunk,
                                             OperatorSinkInput &input) const {
	auto &gstate = input.global_state.Cast<SampleGlobalSinkState>();
	if (!gstate.sample) {
		return SinkResultType::FINISHED;
	}
	// we implement reservoir sampling without replacement and exponential jumps here
	// the algorithm is adopted from the paper Weighted random sampling with a reservoir by Pavlos S. Efraimidis et al.
	// note that the original algorithm is about weighted sampling; this is a simplified approach for uniform sampling
	lock_guard<mutex> glock(gstate.lock);
	gstate.sample->AddToReservoir(chunk);
	return SinkResultType::NEED_MORE_INPUT;
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
SourceResultType PhysicalReservoirSample::GetData(ExecutionContext &context, DataChunk &chunk,
                                                  OperatorSourceInput &input) const {
	auto &sink = this->sink_state->Cast<SampleGlobalSinkState>();
	if (!sink.sample) {
		return SourceResultType::FINISHED;
	}
	auto sample_chunk = sink.sample->GetChunk();
	if (!sample_chunk) {
		return SourceResultType::FINISHED;
	}
	chunk.Move(*sample_chunk);

	return SourceResultType::HAVE_MORE_OUTPUT;
}

string PhysicalReservoirSample::ParamsToString() const {
	return options->sample_size.ToString() + (options->is_percentage ? "%" : " rows");
}

} // namespace duckdb
