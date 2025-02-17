#include "duckdb/execution/operator/helper/physical_streaming_sample.hpp"
#include "duckdb/common/random_engine.hpp"
#include "duckdb/common/to_string.hpp"
#include "duckdb/common/enum_util.hpp"

namespace duckdb {

PhysicalStreamingSample::PhysicalStreamingSample(vector<LogicalType> types, unique_ptr<SampleOptions> options,
                                                 idx_t estimated_cardinality)
    : PhysicalOperator(PhysicalOperatorType::STREAMING_SAMPLE, std::move(types), estimated_cardinality),
      sample_options(std::move(options)) {
	percentage = sample_options->sample_size.GetValue<double>() / 100;
}

//===--------------------------------------------------------------------===//
// Operator
//===--------------------------------------------------------------------===//
class StreamingSampleOperatorState : public OperatorState {
public:
	explicit StreamingSampleOperatorState(int64_t seed) : random(seed) {
	}

	RandomEngine random;
};

void PhysicalStreamingSample::SystemSample(DataChunk &input, DataChunk &result, OperatorState &state_p) const {
	// system sampling: we throw one dice per chunk
	auto &state = state_p.Cast<StreamingSampleOperatorState>();
	double rand = state.random.NextRandom();
	if (rand <= percentage) {
		// rand is smaller than sample_size: output chunk
		result.Reference(input);
	}
}

void PhysicalStreamingSample::BernoulliSample(DataChunk &input, DataChunk &result, OperatorState &state_p) const {
	// bernoulli sampling: we throw one dice per tuple
	// then slice the result chunk
	auto &state = state_p.Cast<StreamingSampleOperatorState>();
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

bool PhysicalStreamingSample::ParallelOperator() const {
	return !(sample_options->repeatable || sample_options->seed.IsValid());
}

unique_ptr<OperatorState> PhysicalStreamingSample::GetOperatorState(ExecutionContext &context) const {
	if (!ParallelOperator()) {
		return make_uniq<StreamingSampleOperatorState>(static_cast<int64_t>(sample_options->seed.GetIndex()));
	}
	RandomEngine random;
	return make_uniq<StreamingSampleOperatorState>(static_cast<int64_t>(random.NextRandomInteger64()));
}

OperatorResultType PhysicalStreamingSample::Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                                    GlobalOperatorState &gstate, OperatorState &state) const {
	switch (sample_options->method) {
	case SampleMethod::BERNOULLI_SAMPLE:
		BernoulliSample(input, chunk, state);
		break;
	case SampleMethod::SYSTEM_SAMPLE:
		SystemSample(input, chunk, state);
		break;
	default:
		throw InternalException("Unsupported sample method for streaming sample");
	}
	return OperatorResultType::NEED_MORE_INPUT;
}

InsertionOrderPreservingMap<string> PhysicalStreamingSample::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	result["Sample Method"] = EnumUtil::ToString(sample_options->method) + ": " + to_string(100 * percentage) + "%";
	return result;
}

} // namespace duckdb
