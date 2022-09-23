#include "duckdb/parser/expression/subquery_expression.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/field_writer.hpp"

namespace duckdb {

SubqueryExpression::SubqueryExpression()
    : ParsedExpression(ExpressionType::SUBQUERY, ExpressionClass::SUBQUERY), subquery_type(SubqueryType::INVALID),
      comparison_type(ExpressionType::INVALID) {
}

string SubqueryExpression::ToString() const {
	switch (subquery_type) {
	case SubqueryType::ANY:
		return child->ToString() + " " + ExpressionTypeToOperator(comparison_type) + " ANY(" + subquery->ToString() +
		       ")";
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

bool SubqueryExpression::Equals(const SubqueryExpression *a, const SubqueryExpression *b) {
	if (!a->subquery || !b->subquery) {
		return false;
	}
	if (!BaseExpression::Equals(a->child.get(), b->child.get())) {
		return false;
	}
	return a->comparison_type == b->comparison_type && a->subquery_type == b->subquery_type &&
	       a->subquery->Equals(b->subquery.get());
}

unique_ptr<ParsedExpression> SubqueryExpression::Copy() const {
	auto copy = make_unique<SubqueryExpression>();
	copy->CopyProperties(*this);
	copy->subquery = unique_ptr_cast<SQLStatement, SelectStatement>(subquery->Copy());
	copy->subquery_type = subquery_type;
	copy->child = child ? child->Copy() : nullptr;
	copy->comparison_type = comparison_type;
	return move(copy);
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

	auto expression = make_unique<SubqueryExpression>();
	expression->subquery_type = subquery_type;
	expression->subquery = move(subquery);
	expression->child = reader.ReadOptional<ParsedExpression>(nullptr);
	expression->comparison_type = reader.ReadRequired<ExpressionType>();
	return move(expression);
}

} // namespace duckdb
