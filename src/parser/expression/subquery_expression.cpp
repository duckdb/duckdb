
#include "parser/expression/subquery_expression.hpp"

#include "common/exception.hpp"
#include "common/serializer.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<Expression> SubqueryExpression::Copy() {
	assert(children.size() == 0);
	assert(!op);
	assert(!context);
	assert(!plan);
	auto copy = make_unique<SubqueryExpression>();
	copy->CopyProperties(*this);
	copy->subquery = subquery->Copy();
	copy->subquery_type = subquery_type;
	return copy;
}

void SubqueryExpression::Serialize(Serializer &serializer) {
	Expression::Serialize(serializer);
	serializer.Write<SubqueryType>(subquery_type);
	subquery->Serialize(serializer);
}

unique_ptr<Expression>
SubqueryExpression::Deserialize(ExpressionDeserializeInfo *info,
                                Deserializer &source) {
	auto subquery_type = source.Read<SubqueryType>();
	auto subquery = QueryNode::Deserialize(source);
	if (info->children.size() > 0) {
		throw SerializationException("Subquery cannot have children!");
	}

	auto expression = make_unique<SubqueryExpression>();
	expression->subquery_type = subquery_type;
	expression->subquery = move(subquery);
	return expression;
}

bool SubqueryExpression::Equals(const Expression *other_) {
	if (!Expression::Equals(other_)) {
		return false;
	}
	auto other = reinterpret_cast<const SubqueryExpression *>(other_);
	if (!other) {
		return false;
	}
	return subquery_type == other->subquery_type &&
	       subquery->Equals(other->subquery.get());
}