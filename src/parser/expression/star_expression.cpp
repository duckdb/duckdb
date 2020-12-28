#include "duckdb/parser/expression/star_expression.hpp"

namespace duckdb {

StarExpression::StarExpression() : ParsedExpression(ExpressionType::STAR, ExpressionClass::STAR) {
}

string StarExpression::ToString() const {
	return "*";
}

unique_ptr<ParsedExpression> StarExpression::Copy() const {
	auto copy = make_unique<StarExpression>();
	copy->CopyProperties(*this);
	return move(copy);
}

unique_ptr<ParsedExpression> StarExpression::Deserialize(ExpressionType type, Deserializer &source) {
	return make_unique<StarExpression>();
}

} // namespace duckdb
