#include "duckdb/parser/expression/collate_expression.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/field_writer.hpp"

#include "duckdb/common/serializer/format_serializer.hpp"
#include "duckdb/common/serializer/format_deserializer.hpp"

namespace duckdb {

CollateExpression::CollateExpression(string collation_p, unique_ptr<ParsedExpression> child)
    : ParsedExpression(ExpressionType::COLLATE, ExpressionClass::COLLATE), collation(std::move(collation_p)) {
	D_ASSERT(child);
	this->child = std::move(child);
}

string CollateExpression::ToString() const {
	return child->ToString() + " COLLATE " + KeywordHelper::WriteOptionallyQuoted(collation);
}

bool CollateExpression::Equal(const CollateExpression *a, const CollateExpression *b) {
	if (!a->child->Equals(b->child.get())) {
		return false;
	}
	if (a->collation != b->collation) {
		return false;
	}
	return true;
}

unique_ptr<ParsedExpression> CollateExpression::Copy() const {
	auto copy = make_uniq<CollateExpression>(collation, child->Copy());
	copy->CopyProperties(*this);
	return std::move(copy);
}

void CollateExpression::Serialize(FieldWriter &writer) const {
	writer.WriteSerializable(*child);
	writer.WriteString(collation);
}

unique_ptr<ParsedExpression> CollateExpression::Deserialize(ExpressionType type, FieldReader &reader) {
	auto child = reader.ReadRequiredSerializable<ParsedExpression>();
	auto collation = reader.ReadRequired<string>();
	return make_uniq_base<ParsedExpression, CollateExpression>(collation, std::move(child));
}

void CollateExpression::FormatSerialize(FormatSerializer &serializer) const {
	ParsedExpression::FormatSerialize(serializer);
	serializer.WriteProperty("child", *child);
	serializer.WriteProperty("collation", collation);
}

unique_ptr<ParsedExpression> CollateExpression::FormatDeserialize(ExpressionType type,
                                                                  FormatDeserializer &deserializer) {
	auto child = deserializer.ReadProperty<unique_ptr<ParsedExpression>>("child");
	auto collation = deserializer.ReadProperty<string>("collation");
	return make_uniq_base<ParsedExpression, CollateExpression>(collation, std::move(child));
}

} // namespace duckdb
