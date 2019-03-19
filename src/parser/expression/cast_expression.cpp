#include "parser/expression/cast_expression.hpp"

#include "common/exception.hpp"

using namespace duckdb;
using namespace std;

void CastExpression::ResolveType() {
	Expression::ResolveType();
	if (child->GetExpressionClass() == ExpressionClass::PARAMETER) {
		child->return_type = return_type;
	}
	ExpressionStatistics::Cast(child->stats, stats);
	if (!stats.FitsInType(return_type)) {
		return_type = stats.MinimalType();
	}
}

unique_ptr<Expression> CastExpression::Copy() const {
	auto copy = make_unique<CastExpression>(return_type, child->Copy());
	copy->CopyProperties(*this);
	return move(copy);
}

void CastExpression::Serialize(Serializer &serializer) {
	Expression::Serialize(serializer);
	child->Serialize(serializer);
}

unique_ptr<Expression> CastExpression::Deserialize(ExpressionType type, TypeId return_type, Deserializer &source) {
	auto child = Expression::Deserialize(source);
	return make_unique_base<Expression, CastExpression>(return_type, move(child));
}

bool CastExpression::Equals(const Expression *other_) const {
	if (!Expression::Equals(other_)) {
		return false;
	}
	auto other = (CastExpression *)other_;
	if (!child->Equals(other->child.get())) {
		return false;
	}
	return true;
}

size_t CastExpression::ChildCount() const {
	return 1;
}

Expression *CastExpression::GetChild(size_t index) const {
	assert(index == 0);
	return child.get();
}

void CastExpression::ReplaceChild(std::function<unique_ptr<Expression>(unique_ptr<Expression> expression)> callback,
                                  size_t index) {
	assert(index == 0);
	child = callback(move(child));
}
