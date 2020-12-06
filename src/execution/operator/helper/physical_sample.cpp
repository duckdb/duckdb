#include "duckdb/execution/operator/helper/physical_sample.hpp"
#include "duckdb/execution/reservoir_sample.hpp"

using namespace std;

namespace duckdb {

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class SampleGlobalOperatorState : public GlobalOperatorState {
public:
	SampleGlobalOperatorState(unique_ptr<BlockingSample> sample) : sample(move(sample)) {}

	//! The lock for updating the global aggregate state
	mutex lock;
	//! The reservoir sample
	unique_ptr<BlockingSample> sample;
};

unique_ptr<GlobalOperatorState> PhysicalSample::GetGlobalState(ClientContext &context) {
	return make_unique<SampleGlobalOperatorState>(make_unique<ReservoirSample>(sample_count));
}

void PhysicalSample::Sink(ExecutionContext &context, GlobalOperatorState &state, LocalSinkState &lstate,
                         DataChunk &input) {
	if (sample_count == 0) {
		return;
	}
	// we implement reservoir sampling without replacement and exponential jumps here
	// the algorithm is adopted from the paper Weighted random sampling with a reservoir by Pavlos S. Efraimidis et al.
	// note that the original algorithm is about weighted sampling; this is a simplified approach for uniform sampling
	auto &gstate = (SampleGlobalOperatorState &)state;
	lock_guard<mutex> glock(gstate.lock);
	gstate.sample->AddToReservoir(input);
}

//===--------------------------------------------------------------------===//
// GetChunkInternal
//===--------------------------------------------------------------------===//
void PhysicalSample::GetChunkInternal(ExecutionContext &context, DataChunk &chunk, PhysicalOperatorState *state_) {
	auto &sink = (SampleGlobalOperatorState &)*this->sink_state;
	auto sample_chunk = sink.sample->GetChunk();
	if (!sample_chunk) {
		return;
	}
	chunk.Reference(*sample_chunk);
}

unique_ptr<PhysicalOperatorState> PhysicalSample::GetOperatorState() {
	return make_unique<PhysicalOperatorState>(*this, children[0].get());
}

string PhysicalSample::ParamsToString() const {
	return to_string(sample_count);
}

} // namespace duckdb
