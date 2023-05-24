#include "duckdb/parser/expression/subquery_expression.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/field_writer.hpp"
#include "duckdb/common/serializer/format_deserializer.hpp"
#include "duckdb/common/serializer/format_serializer.hpp"

namespace duckdb {

SubqueryExpression::SubqueryExpression()
    : ParsedExpression(ExpressionType::SUBQUERY, ExpressionClass::SUBQUERY), subquery_type(SubqueryType::INVALID),
      comparison_type(ExpressionType::INVALID) {
}

string SubqueryExpression::ToString() const {
	switch (subquery_type) {
	case SubqueryType::ANY:
		return "(" + child->ToString() + " " + ExpressionTypeToOperator(comparison_type) + " ANY(" +
		       subquery->ToString() + "))";
	case SubqueryType::EXISTS:
		return "EXISTS(" + subquery->ToString() + ")";
	case SubqueryType::NOT_EXISTS:
		return "NOT EXISTS(" + subquery->ToString() + ")";
	case SubqueryType::SCALAR:
		return "(" + subquery->ToString() + ")";
	default:
		throw InternalException("Unrecognized type for subquery");
	}
}

bool SubqueryExpression::Equal(const SubqueryExpression &a, const SubqueryExpression &b) {
	if (!a.subquery || !b.subquery) {
		return false;
	}
	if (!ParsedExpression::Equals(a.child, b.child)) {
		return false;
	}
	return a.comparison_type == b.comparison_type && a.subquery_type == b.subquery_type &&
	       a.subquery->Equals(*b.subquery);
}

unique_ptr<ParsedExpression> SubqueryExpression::Copy() const {
	auto copy = make_uniq<SubqueryExpression>();
	copy->CopyProperties(*this);
	copy->subquery = unique_ptr_cast<SQLStatement, SelectStatement>(subquery->Copy());
	copy->subquery_type = subquery_type;
	copy->child = child ? child->Copy() : nullptr;
	copy->comparison_type = comparison_type;
	return std::move(copy);
}

void SubqueryExpression::Serialize(FieldWriter &writer) const {
	auto &serializer = writer.GetSerializer();

	writer.WriteField<SubqueryType>(subquery_type);
	// FIXME: this shouldn't use a serializer (probably)?
	subquery->Serialize(serializer);
	writer.WriteOptional(child);
	writer.WriteField<ExpressionType>(comparison_type);
}

unique_ptr<ParsedExpression> SubqueryExpression::Deserialize(ExpressionType type, FieldReader &reader) {
	// FIXME: this shouldn't use a source
	auto &source = reader.GetSource();

	auto subquery_type = reader.ReadRequired<SubqueryType>();
	auto subquery = SelectStatement::Deserialize(source);

	auto expression = make_uniq<SubqueryExpression>();
	expression->subquery_type = subquery_type;
	expression->subquery = std::move(subquery);
	expression->child = reader.ReadOptional<ParsedExpression>(nullptr);
	expression->comparison_type = reader.ReadRequired<ExpressionType>();
	return std::move(expression);
}

void SubqueryExpression::FormatSerialize(FormatSerializer &serializer) const {
	ParsedExpression::FormatSerialize(serializer);
	serializer.WriteProperty("subquery_type", subquery_type);
	serializer.WriteProperty("subquery", *subquery);
	serializer.WriteOptionalProperty("child", child);
	serializer.WriteProperty("comparison_type", comparison_type);
}

unique_ptr<ParsedExpression> SubqueryExpression::FormatDeserialize(ExpressionType type,
                                                                   FormatDeserializer &deserializer) {
	auto expression = make_uniq<SubqueryExpression>();
	deserializer.ReadProperty("subquery_type", expression->subquery_type);
	deserializer.ReadProperty("subquery", expression->subquery);
	deserializer.ReadOptionalProperty("child", expression->child);
	deserializer.ReadProperty("comparison_type", expression->comparison_type);
	return std::move(expression);
}

} // namespace duckdb
