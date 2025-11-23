#include "duckdb/parser/expression/default_expression.hpp"

namespace duckdb {

DefaultExpression::DefaultExpression() : ParsedExpression(ExpressionType::VALUE_DEFAULT, ExpressionClass::DEFAULT) {
}

string DefaultExpression::ToString() const {
	return "DEFAULT";
}

unique_ptr<ParsedExpression> DefaultExpression::Copy() const {
	auto copy = make_uniq<DefaultExpression>();
	copy->CopyProperties(*this);
	return std::move(copy);
}

} // namespace duckdb
