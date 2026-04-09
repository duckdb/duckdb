#include <memory>
#include <utility>

#include "core_functions/scalar/generic_functions.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/function/function.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/planner/expression.hpp"

namespace duckdb {
class DataChunk;
class Vector;
struct ExpressionState;

namespace {

void CastToTypeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	throw InternalException("CastToType function cannot be executed directly");
}

unique_ptr<Expression> BindCastToTypeFunction(FunctionBindExpressionInput &input) {
	auto &return_type = input.children[1]->GetReturnType();
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
	fun.SetBindExpressionCallback(BindCastToTypeFunction);
	return fun;
}

} // namespace duckdb
