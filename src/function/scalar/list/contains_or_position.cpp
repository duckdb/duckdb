#include "duckdb/function/scalar/list_functions.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression_binder.hpp"
#include "duckdb/function/scalar/list/contains_or_position.hpp"

namespace duckdb {

template <bool RETURN_POSITION>
static void ListSearchFunction(DataChunk &input, ExpressionState &state, Vector &result) {
	auto target_count = input.size();
	auto &list_vec = input.data[0];
	auto &source_vec = ListVector::GetEntry(list_vec);
	auto &target_vec = input.data[1];

	ListSearchOp<RETURN_POSITION>(list_vec, source_vec, target_vec, result, target_count);

	if (target_count == 1) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

static unique_ptr<FunctionData> ListSearchBind(ClientContext &context, ScalarFunction &bound_function,
                                               vector<unique_ptr<Expression>> &arguments) {
	D_ASSERT(bound_function.arguments.size() == 2);

	// If the first argument is an array, cast it to a list
	arguments[0] = BoundCastExpression::AddArrayCastToList(context, std::move(arguments[0]));

	const auto &list = arguments[0]->return_type;
	const auto &value = arguments[1]->return_type;

	const auto list_is_param = list.id() == LogicalTypeId::UNKNOWN;
	const auto value_is_param = value.id() == LogicalTypeId::UNKNOWN;

	if (list_is_param) {
		if (!value_is_param) {
			// only list is a parameter, cast it to a list of value type
			bound_function.arguments[0] = LogicalType::LIST(value);
			bound_function.arguments[1] = value;
		}
	} else if (value_is_param) {
		// only value is a parameter: we expect the child type of list
		bound_function.arguments[0] = list;
		bound_function.arguments[1] = ListType::GetChildType(list);
	} else {
		LogicalType max_child_type;
		if (!LogicalType::TryGetMaxLogicalType(context, ListType::GetChildType(list), value, max_child_type)) {
			throw BinderException(
			    "%s: Cannot match element of type '%s' in a list of type '%s' - an explicit cast is required",
			    bound_function.name, value.ToString(), list.ToString());
		}

		bound_function.arguments[0] = LogicalType::LIST(max_child_type);
		bound_function.arguments[1] = max_child_type;
	}
	return make_uniq<VariableReturnBindData>(bound_function.return_type);
}

ScalarFunction ListContainsFun::GetFunction() {
	return ScalarFunction({LogicalType::LIST(LogicalType::ANY), LogicalType::ANY}, LogicalType::BOOLEAN,
	                      ListSearchFunction<false>, ListSearchBind);
}

ScalarFunction ListPositionFun::GetFunction() {
	return ScalarFunction({LogicalType::LIST(LogicalType::ANY), LogicalType::ANY}, LogicalType::INTEGER,
	                      ListSearchFunction<true>, ListSearchBind);
}

} // namespace duckdb
