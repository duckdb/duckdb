#include "duckdb/parser/expression/default_expression.hpp"

#include "duckdb/common/exception.hpp"

namespace duckdb {

DefaultExpression::DefaultExpression() : ParsedExpression(ExpressionType::VALUE_DEFAULT, ExpressionClass::DEFAULT) {
}

string DefaultExpression::ToString() const {
	return "DEFAULT";
}

unique_ptr<ParsedExpression> DefaultExpression::Copy() const {
	auto copy = make_unique<DefaultExpression>();
	copy->CopyProperties(*this);
	return std::move(copy);
}

void DefaultExpression::Serialize(FieldWriter &writer) const {
}

unique_ptr<ParsedExpression> DefaultExpression::Deserialize(ExpressionType type, FieldReader &source) {
	return make_unique<DefaultExpression>();
}

} // namespace duckdb
