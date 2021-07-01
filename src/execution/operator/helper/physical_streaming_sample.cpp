#include "duckdb/execution/operator/helper/physical_streaming_sample.hpp"
#include "duckdb/common/random_engine.hpp"
#include "duckdb/common/to_string.hpp"

namespace duckdb {

PhysicalStreamingSample::PhysicalStreamingSample(vector<LogicalType> types, SampleMethod method, double percentage,
                                                 int64_t seed, idx_t estimated_cardinality)
    : PhysicalOperator(PhysicalOperatorType::STREAMING_SAMPLE, move(types), estimated_cardinality), method(method),
      percentage(percentage / 100), seed(seed) {
}

//===--------------------------------------------------------------------===//
// GetChunkInternal
//===--------------------------------------------------------------------===//
class StreamingSampleOperatorState : public PhysicalOperatorState {
public:
	StreamingSampleOperatorState(PhysicalOperator &op, PhysicalOperator *child, int64_t seed)
	    : PhysicalOperatorState(op, child), random(seed) {
	}

	RandomEngine random;
};

void PhysicalStreamingSample::SystemSample(DataChunk &input, DataChunk &result, PhysicalOperatorState *state_p) const {
	// system sampling: we throw one dice per chunk
	auto &state = (StreamingSampleOperatorState &)*state_p;
	double rand = state.random.NextRandom();
	if (rand <= percentage) {
		// rand is smaller than sample_size: output chunk
		result.Reference(input);
	}
}

void PhysicalStreamingSample::BernoulliSample(DataChunk &input, DataChunk &result,
                                              PhysicalOperatorState *state_p) const {
	// bernoulli sampling: we throw one dice per tuple
	// then slice the result chunk
	auto &state = (StreamingSampleOperatorState &)*state_p;
	idx_t result_count = 0;
	SelectionVector sel(STANDARD_VECTOR_SIZE);
	for (idx_t i = 0; i < input.size(); i++) {
		double rand = state.random.NextRandom();
		if (rand <= percentage) {
			sel.set_index(result_count++, i);
		}
	}
	if (result_count > 0) {
		result.Slice(input, sel, result_count);
	}
}

void PhysicalStreamingSample::GetChunkInternal(ExecutionContext &context, DataChunk &chunk,
                                               PhysicalOperatorState *state) const {

	// get the next chunk from the child
	do {
		children[0]->GetChunk(context, state->child_chunk, state->child_state.get());
		if (state->child_chunk.size() == 0) {
			return;
		}

		switch (method) {
		case SampleMethod::BERNOULLI_SAMPLE:
			BernoulliSample(state->child_chunk, chunk, state);
			break;
		case SampleMethod::SYSTEM_SAMPLE:
			SystemSample(state->child_chunk, chunk, state);
			break;
		default:
			throw InternalException("Unsupported sample method for streaming sample");
		}
	} while (chunk.size() == 0);
}

unique_ptr<PhysicalOperatorState> PhysicalStreamingSample::GetOperatorState() {
	return make_unique<StreamingSampleOperatorState>(*this, children[0].get(), seed);
}

string PhysicalStreamingSample::ParamsToString() const {
	return SampleMethodToString(method) + ": " + to_string(100 * percentage) + "%";
}

} // namespace duckdb
