#include "duckdb/parser/expression/default_expression.hpp"

#include "duckdb/common/exception.hpp"

#include "duckdb/common/serializer/format_serializer.hpp"
#include "duckdb/common/serializer/format_deserializer.hpp"

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

void DefaultExpression::Serialize(FieldWriter &writer) const {
}

unique_ptr<ParsedExpression> DefaultExpression::Deserialize(ExpressionType type, FieldReader &source) {
	return make_uniq<DefaultExpression>();
}

void DefaultExpression::FormatSerialize(FormatSerializer &serializer) const {
	ParsedExpression::FormatSerialize(serializer);
}

} // namespace duckdb
