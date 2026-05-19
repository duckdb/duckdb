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

unique_ptr<ParsedExpression> ParameterExpression::Copy() const {
	auto copy = make_uniq<ParameterExpression>();
	copy->identifier = identifier;
	copy->CopyProperties(*this);
	return std::move(copy);
}

} // namespace duckdb
