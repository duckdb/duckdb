#include "duckdb/function/scalar/list_functions.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression_binder.hpp"
#include "duckdb/function/scalar/list/contains_or_position.hpp"

namespace duckdb {

template <class RETURN_TYPE, bool FIND_NULLS = false>
static void ListSearchFunction(DataChunk &input, ExpressionState &state, Vector &result) {
	if (result.GetType().id() == LogicalTypeId::SQLNULL) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
		ConstantVector::SetNull(result, true);
		return;
	}

	auto target_count = input.size();
	auto &input_list = input.data[0];
	auto &list_child = ListVector::GetEntry(input_list);
	auto &target = input.data[1];

	ListSearchOp<RETURN_TYPE, FIND_NULLS>(input_list, list_child, target, result, target_count);

	if (target_count == 1) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

ScalarFunction ListContainsFun::GetFunction() {
	return ScalarFunction({LogicalType::LIST(LogicalType::TEMPLATE("T")), LogicalType::TEMPLATE("T")},
	                      LogicalType::BOOLEAN, ListSearchFunction<bool>);
}

ScalarFunction ListPositionFun::GetFunction() {
	auto fun = ScalarFunction({LogicalType::LIST(LogicalType::TEMPLATE("T")), LogicalType::TEMPLATE("T")},
	                          LogicalType::INTEGER, ListSearchFunction<int32_t, true>);
	fun.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	return fun;
}

} // namespace duckdb
