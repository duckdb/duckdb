#include "core_functions/scalar/generic_functions.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/common/type_visitor.hpp"

namespace duckdb {

static void ReplaceTypeFunction(DataChunk &, ExpressionState &, Vector &) {
	throw InternalException("ReplaceTypeFunction function cannot be executed directly");
}

unique_ptr<Expression> BindReplaceTypeFunction(FunctionBindExpressionInput &input) {
	const auto &from = input.children[1]->return_type;
	const auto &to = input.children[2]->return_type;
	if (from.id() == LogicalTypeId::UNKNOWN || to.id() == LogicalTypeId::UNKNOWN) {
		// parameters - unknown return type
		throw ParameterNotResolvedException();
	}
	if (to.id() == LogicalTypeId::SQLNULL) {
		throw InvalidInputException("replace_type cannot be used to replace type with NULL");
	}
	const auto return_type = TypeVisitor::VisitReplace(
	    input.children[0]->return_type, [&from, &to](const LogicalType &type) { return type == from ? to : type; });
	return BoundCastExpression::AddCastToType(input.context, std::move(input.children[0]), return_type);
}

ScalarFunction ReplaceTypeFun::GetFunction() {
	auto fun =
	    ScalarFunction({LogicalType::ANY, LogicalType::ANY, LogicalType::ANY}, LogicalType::ANY, ReplaceTypeFunction);
	fun.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	fun.bind_expression = BindReplaceTypeFunction;
	return fun;
}

} // namespace duckdb
