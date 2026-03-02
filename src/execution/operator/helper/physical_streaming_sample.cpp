#include "duckdb/execution/operator/helper/physical_streaming_sample.hpp"
#include "duckdb/common/random_engine.hpp"
#include "duckdb/common/to_string.hpp"
#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/numeric_utils.hpp"

namespace duckdb {

PhysicalStreamingSample::PhysicalStreamingSample(PhysicalPlan &physical_plan, vector<LogicalType> types,
                                                 unique_ptr<SampleOptions> options, idx_t estimated_cardinality)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::STREAMING_SAMPLE, std::move(types), estimated_cardinality),
      sample_options(std::move(options)), percentage(0.0), rows(0) {
	if (sample_options->is_percentage) {
		percentage = sample_options->sample_size.GetValue<double>() / 100;
	} else {
		// Convert target row count to a sampling rate.
		// Prefer the pre-calculated sample_rate from the planner if available (ensures
		// consistency with pushdown path), otherwise derive from estimated_cardinality.
		// Fallback to 1.0 (take all rows) if no estimate is available.
		rows = NumericCast<idx_t>(sample_options->sample_size.GetValue<int64_t>());
		if (sample_options->sample_rate > 0) {
			percentage = sample_options->sample_rate;
		} else if (estimated_cardinality > 0) {
			percentage = static_cast<double>(rows) / static_cast<double>(estimated_cardinality);
			if (percentage > 1.0) {
				percentage = 1.0;
			}
		} else {
			percentage = 1.0;
		}
	}
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

void PhysicalStreamingSample::SystemSamplePercent(DataChunk &input, DataChunk &result, OperatorState &state_p) const {
	// system sampling: we throw one dice per chunk
	auto &state = state_p.Cast<StreamingSampleOperatorState>();
	double rand = state.random.NextRandom();
	if (rand <= percentage) {
		// rand is smaller than sample_size: output chunk
		result.Reference(input);
	}
}

void PhysicalStreamingSample::SystemSampleRows(DataChunk &input, DataChunk &result, GlobalOperatorState &gstate_p,
                                               OperatorState &state_p) const {
	double rate = percentage;
	if (rate <= 0) {
		return;
	}

	// We take rows from the beginning of the chunk rather than randomly
	// selecting positions to keep sampling fast and consistent.
	idx_t rows_to_take = static_cast<idx_t>(std::ceil(rate * static_cast<double>(input.size())));
	if (rows_to_take < 1) {
		rows_to_take = 1;
	}
	if (rows_to_take > input.size()) {
		rows_to_take = input.size();
	}
	SelectionVector sel(0, rows_to_take);
	result.Slice(input, sel, rows_to_take);
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
		if (sample_options->is_percentage) {
			SystemSamplePercent(input, chunk, state);
		} else {
			SystemSampleRows(input, chunk, gstate, state);
		}
		break;
	default:
		throw InternalException("Unsupported sample method for streaming sample");
	}
	return OperatorResultType::NEED_MORE_INPUT;
}

InsertionOrderPreservingMap<string> PhysicalStreamingSample::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	if (sample_options->is_percentage) {
		result["Sample Method"] = EnumUtil::ToString(sample_options->method) + ": " + to_string(100 * percentage) + "%";
	} else {
		result["Sample Method"] = EnumUtil::ToString(sample_options->method) + ": " + to_string(rows) + " rows";
	}
	return result;
}

} // namespace duckdb
