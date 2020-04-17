#include "duckdb/parser/expression/subquery_expression.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/serializer.hpp"

using namespace duckdb;
using namespace std;

SubqueryExpression::SubqueryExpression()
    : ParsedExpression(ExpressionType::SUBQUERY, ExpressionClass::SUBQUERY), subquery_type(SubqueryType::INVALID),
      comparison_type(ExpressionType::INVALID) {
}

string SubqueryExpression::ToString() const {
	return "SUBQUERY";
}

bool SubqueryExpression::Equals(const SubqueryExpression *a, const SubqueryExpression *b) {
	if (!a->subquery || !b->subquery) {
		return false;
	}
	return a->subquery_type == b->subquery_type && a->subquery->Equals(b->subquery.get());
}

unique_ptr<ParsedExpression> SubqueryExpression::Copy() const {
	auto copy = make_unique<SubqueryExpression>();
	copy->CopyProperties(*this);
	copy->subquery = subquery->Copy();
	copy->subquery_type = subquery_type;
	copy->child = child ? child->Copy() : nullptr;
	copy->comparison_type = comparison_type;
	return move(copy);
}

void SubqueryExpression::Serialize(Serializer &serializer) {
	ParsedExpression::Serialize(serializer);
	serializer.Write<SubqueryType>(subquery_type);
	subquery->Serialize(serializer);
	serializer.WriteOptional(child);
	serializer.Write<ExpressionType>(comparison_type);
}

unique_ptr<ParsedExpression> SubqueryExpression::Deserialize(ExpressionType type, Deserializer &source) {
	auto subquery_type = source.Read<SubqueryType>();
	auto subquery = QueryNode::Deserialize(source);

	auto expression = make_unique<SubqueryExpression>();
	expression->subquery_type = subquery_type;
	expression->subquery = move(subquery);
	expression->child = source.ReadOptional<ParsedExpression>();
	expression->comparison_type = source.Read<ExpressionType>();
	return move(expression);
}
