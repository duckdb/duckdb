#include "duckdb/parser/expression/parameter_expression.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/hash.hpp"

namespace duckdb {

ParameterExpression::ParameterExpression()
    : ParsedExpression(ExpressionType::VALUE_PARAMETER, ExpressionClass::PARAMETER) {
}

string ParameterExpression::ToString() const {
	return "$" + identifier;
}

} // namespace duckdb
