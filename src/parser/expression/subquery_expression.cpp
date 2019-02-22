#include "parser/expression/subquery_expression.hpp"

#include "common/exception.hpp"
#include "common/serializer.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<Expression> SubqueryExpression::Copy() {
	auto copy = make_unique<SubqueryExpression>();
	copy->CopyProperties(*this);
	copy->subquery = subquery->Copy();
	copy->subquery_type = subquery_type;
	copy->child = child ? child->Copy() : nullptr;
	copy->comparison_type = comparison_type;
	return copy;
}

void SubqueryExpression::Serialize(Serializer &serializer) {
	Expression::Serialize(serializer);
	serializer.Write<SubqueryType>(subquery_type);
	subquery->Serialize(serializer);
	serializer.WriteOptional(child);
	serializer.Write<ExpressionType>(comparison_type);
}

unique_ptr<Expression> SubqueryExpression::Deserialize(ExpressionType type, TypeId return_type, Deserializer &source) {
	auto subquery_type = source.Read<SubqueryType>();
	auto subquery = QueryNode::Deserialize(source);

	auto expression = make_unique<SubqueryExpression>();
	expression->subquery_type = subquery_type;
	expression->subquery = move(subquery);
	expression->child = source.ReadOptional<Expression>();
	expression->comparison_type = source.Read<ExpressionType>();
	return expression;
}

bool SubqueryExpression::Equals(const Expression *other_) const {
	if (!Expression::Equals(other_)) {
		return false;
	}
	auto other = reinterpret_cast<const SubqueryExpression *>(other_);
	if (!other) {
		return false;
	}
	if (!subquery || !other->subquery) {
		return false;
	}
	return subquery_type == other->subquery_type && subquery->Equals(other->subquery.get());
}

size_t SubqueryExpression::ChildCount() const {
	return child ? 1 : 0;
}

Expression *SubqueryExpression::GetChild(size_t index) const {
	assert(index == 0 && child);
	return child.get();
}

void SubqueryExpression::ReplaceChild(std::function<unique_ptr<Expression>(unique_ptr<Expression> expression)> callback,
                                      size_t index) {
	assert(index == 0 && child);
	child = callback(move(child));
}
