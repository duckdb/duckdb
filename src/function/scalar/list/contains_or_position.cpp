#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression_binder.hpp"
#include "duckdb/common/operator/comparison_operators.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"

namespace duckdb {

static void ListContainsFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	(void)state;
	return ListContainsOrPosition<bool, ContainsFunctor, ListArgFunctor>(args, result);
}

static void ListPositionFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	(void)state;
	return ListContainsOrPosition<int32_t, PositionFunctor, ListArgFunctor>(args, result);
}

template <LogicalTypeId RETURN_TYPE>
static unique_ptr<FunctionData> ListContainsOrPositionBind(ClientContext &context, ScalarFunction &bound_function,
                                                           vector<unique_ptr<Expression>> &arguments) {
	D_ASSERT(bound_function.arguments.size() == 2);

	// If the first argument is an array, cast it to a list
	arguments[0] = BoundCastExpression::AddArrayCastToList(context, std::move(arguments[0]));

	const auto &list = arguments[0]->return_type; // change to list
	const auto &value = arguments[1]->return_type;
	if (list.id() == LogicalTypeId::UNKNOWN) {
		bound_function.return_type = RETURN_TYPE;
		if (value.id() != LogicalTypeId::UNKNOWN) {
			// only list is a parameter, cast it to a list of value type
			bound_function.arguments[0] = LogicalType::LIST(value);
			bound_function.arguments[1] = value;
		}
	} else if (value.id() == LogicalTypeId::UNKNOWN) {
		// only value is a parameter: we expect the child type of list
		auto const &child_type = ListType::GetChildType(list);
		bound_function.arguments[0] = list;
		bound_function.arguments[1] = child_type;
		bound_function.return_type = RETURN_TYPE;
	} else {
		auto const &child_type = ListType::GetChildType(list);
		LogicalType max_child_type;
		if (!LogicalType::TryGetMaxLogicalType(context, child_type, value, max_child_type)) {
			throw BinderException(
			    "Cannot get list_position of element of type %s in a list of type %s[] - an explicit cast is required",
			    value.ToString(), child_type.ToString());
		}
		auto list_type = LogicalType::LIST(max_child_type);

		bound_function.arguments[0] = list_type;
		bound_function.arguments[1] = value == max_child_type ? value : max_child_type;

		// list_contains and list_position only differ in their return type
		bound_function.return_type = RETURN_TYPE;
	}
	return make_uniq<VariableReturnBindData>(bound_function.return_type);
}

static unique_ptr<FunctionData> ListContainsBind(ClientContext &context, ScalarFunction &bound_function,
                                                 vector<unique_ptr<Expression>> &arguments) {
	return ListContainsOrPositionBind<LogicalType::BOOLEAN>(context, bound_function, arguments);
}

static unique_ptr<FunctionData> ListPositionBind(ClientContext &context, ScalarFunction &bound_function,
                                                 vector<unique_ptr<Expression>> &arguments) {
	return ListContainsOrPositionBind<LogicalType::INTEGER>(context, bound_function, arguments);
}

ScalarFunction ListContainsFun::GetFunction() {
	return ScalarFunction({LogicalType::LIST(LogicalType::ANY), LogicalType::ANY}, // argument list
	                      LogicalType::BOOLEAN,                                    // return type
	                      ListContainsFunction, ListContainsBind, nullptr);
}

ScalarFunction ListPositionFun::GetFunction() {
	return ScalarFunction({LogicalType::LIST(LogicalType::ANY), LogicalType::ANY}, // argument list
	                      LogicalType::INTEGER,                                    // return type
	                      ListPositionFunction, ListPositionBind, nullptr);
}

void ListContainsFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction({"list_contains", "array_contains", "list_has", "array_has"}, GetFunction());
}

void ListPositionFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction({"list_position", "list_indexof", "array_position", "array_indexof"}, GetFunction());
}
} // namespace duckdb
