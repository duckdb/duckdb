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

static unique_ptr<FunctionData> ListSearchBind(ClientContext &context, ScalarFunction &bound_function,
                                               vector<unique_ptr<Expression>> &arguments) {
	D_ASSERT(bound_function.arguments.size() == 2);

	// If the first argument is an array, cast it to a list
	arguments[0] = BoundCastExpression::AddArrayCastToList(context, std::move(arguments[0]));

	const auto &list = arguments[0]->return_type;
	const auto &value = arguments[1]->return_type;

	if (list.id() == LogicalTypeId::SQLNULL) {
		bound_function.arguments[0] = LogicalTypeId::UNKNOWN;
		bound_function.arguments[1] = LogicalTypeId::UNKNOWN;
		bound_function.return_type = LogicalType::SQLNULL;
		return make_uniq<VariableReturnBindData>(bound_function.return_type);
	}

	if (list.IsUnknown() && value.IsUnknown()) {
		bound_function.arguments[0] = list;
		bound_function.arguments[1] = value;
		return nullptr;
	}

	if (list.IsUnknown()) {
		// Only the list type is unknown.
		// We can infer its type from the type of the value.
		bound_function.arguments[0] = LogicalType::LIST(value);
		bound_function.arguments[1] = value;
	} else if (value.IsUnknown()) {
		// Only the value type is unknown.
		// We can infer its type from the child type of the list.
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
	                      ListSearchFunction<bool>, ListSearchBind);
}

ScalarFunction ListPositionFun::GetFunction() {
	auto fun = ScalarFunction({LogicalType::LIST(LogicalType::ANY), LogicalType::ANY}, LogicalType::INTEGER,
	                          ListSearchFunction<int32_t, true>, ListSearchBind);
	fun.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	return fun;
}

} // namespace duckdb
