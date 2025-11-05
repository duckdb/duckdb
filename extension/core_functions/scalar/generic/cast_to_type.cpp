#include "core_functions/scalar/generic_functions.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"

namespace duckdb {

namespace {

void CastToTypeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	throw InternalException("CastToType function cannot be executed directly");
}

unique_ptr<Expression> BindCastToTypeFunction(FunctionBindExpressionInput &input) {
	auto &return_type = input.children[1]->return_type;
	if (return_type.id() == LogicalTypeId::UNKNOWN) {
		// parameter - unknown return type
		throw ParameterNotResolvedException();
	}
	if (return_type.id() == LogicalTypeId::SQLNULL) {
		throw InvalidInputException("cast_to_type cannot be used to cast to NULL");
	}
	return BoundCastExpression::AddCastToType(input.context, std::move(input.children[0]), return_type);
}

} // namespace
ScalarFunction CastToTypeFun::GetFunction() {
	auto fun = ScalarFunction({LogicalType::ANY, LogicalType::ANY}, LogicalType::ANY, CastToTypeFunction);
	fun.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	fun.bind_expression = BindCastToTypeFunction;
	return fun;
}

} // namespace duckdb
