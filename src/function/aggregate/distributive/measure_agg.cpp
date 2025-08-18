#include "duckdb/function/aggregate/distributive_functions.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/common/algorithm.hpp"
#include "duckdb/common/extra_type_info.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/function/aggregate_function.hpp"

namespace duckdb {

namespace {

struct AggState {};

struct AggOperation {
	template <class STATE>
	static void Initialize(STATE &state) {
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void ConstantOperation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &unary_input,
	                              idx_t count) {
		throw InternalException("MEASURE_AGG should be rewritten and never evaluated directly.");
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void Operation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &unary_input) {
		throw InternalException("MEASURE_AGG should be rewritten and never evaluated directly.");
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE &target, AggregateInputData &) {
		throw InternalException("MEASURE_AGG should be rewritten and never evaluated directly.");
	}

	template <class TARGET_TYPE, class STATE>
	static void Finalize(STATE &state, TARGET_TYPE &target, AggregateFinalizeData &finalize_data) {
		throw InternalException("MEASURE_AGG should be rewritten and never evaluated directly.");
	}

	static bool IgnoreNull() {
		throw InternalException("MEASURE_AGG should be rewritten and never evaluated directly.");
	}
};

unique_ptr<FunctionData> BindMeasureAgg(ClientContext &context, AggregateFunction &function,
                                        vector<unique_ptr<Expression>> &arguments) {
	if (arguments.size() != 1) {
		throw InvalidInputException("MEASURE_AGG takes a single value with a measure type.");
	}

	auto input_type = arguments[0]->return_type;
	auto name = std::move(function.name);

	if (input_type.id() != LogicalTypeId::MEASURE_TYPE) {
		throw InvalidInputException("MEASURE_AGG takes a single value with a measure type.");
	}

	// For measure types, the output type should be the measure output type
	auto &measure_info = input_type.GetAuxInfoShrPtr()->Cast<MeasureTypeInfo>();
	auto measure_output_type = measure_info.measure_output_type;

	function = AggregateFunction::UnaryAggregate<AggState, int32_t, int32_t, AggOperation>(
	    input_type, measure_output_type, FunctionNullHandling::DEFAULT_NULL_HANDLING);
	function.name = std::move(name);
	function.distinct_dependent = AggregateDistinctDependent::NOT_DISTINCT_DEPENDENT;
	function.return_type = measure_output_type;

	return nullptr;
}

} // namespace

AggregateFunctionSet MeasureAggFun::GetFunctions() {
	AggregateFunctionSet set("measure_agg");

	// MEASURE_AGG accepts ANY type and binds dynamically
	set.AddFunction(AggregateFunction({LogicalType::ANY}, LogicalType::ANY, nullptr, nullptr, nullptr, nullptr, nullptr,
	                                  nullptr, BindMeasureAgg));

	return set;
}
} // namespace duckdb
