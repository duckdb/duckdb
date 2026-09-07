#include "duckdb/function/scalar/generic_functions.hpp"

#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/unsafe_barrier.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/planner/expression_iterator.hpp"

namespace duckdb {

namespace {

void UnsafeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	result.Reference(args.data[0]);
	// flatten the result - a barrier must not hand out a dictionary whose child still holds the values of rows that
	// were filtered out before this expression was evaluated
	result.Flatten();
}

unique_ptr<FunctionData> UnsafeBind(BindScalarFunctionInput &input) {
	auto &bound_function = input.GetBoundFunction();
	auto &arguments = input.GetArguments();
	bound_function.SetReturnType(arguments[0]->GetReturnType());
	return make_uniq<VariableReturnBindData>(bound_function.GetReturnType());
}

ScalarFunction GetUnsafeFunction(const LogicalType &type) {
	ScalarFunction fun(UnsafeFun::Name, {type}, type, UnsafeFunction);
	fun.SetBindCallback(UnsafeBind);
	// a barrier must survive binding - it must not be folded away when the argument is (typed as) NULL
	fun.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	// the barrier itself never throws, but marking it fallible keeps the expression out of every optimization that
	// evaluates an expression on more rows than it would otherwise see (dictionary execution, filter reordering)
	fun.SetFallible();
	return fun;
}

} // namespace

ScalarFunction UnsafeFun::GetFunction() {
	return GetUnsafeFunction(LogicalType::ANY);
}

bool UnsafeBarrier::IsBarrier(const Expression &expr) {
	if (expr.GetExpressionClass() != ExpressionClass::BOUND_FUNCTION) {
		return false;
	}
	return expr.Cast<BoundFunctionExpression>().Function().GetName() == UnsafeFun::Name;
}

bool UnsafeBarrier::Contains(const Expression &expr) {
	if (IsBarrier(expr)) {
		return true;
	}
	bool contains = false;
	ExpressionIterator::EnumerateChildren(expr, [&](const Expression &child) { contains |= Contains(child); });
	return contains;
}

bool UnsafeBarrier::Required(const Expression &expr) {
	return expr.CanThrow() || expr.IsVolatile() || expr.HasSubquery();
}

unique_ptr<Expression> UnsafeBarrier::Wrap(unique_ptr<Expression> expr) {
	auto return_type = expr->GetReturnType();
	auto function = GetUnsafeFunction(return_type);

	vector<unique_ptr<Expression>> arguments;
	arguments.push_back(std::move(expr));
	return make_uniq<BoundFunctionExpression>(BoundScalarFunction(function), std::move(arguments),
	                                          make_uniq<VariableReturnBindData>(return_type));
}

} // namespace duckdb
