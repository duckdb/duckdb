#include "parser/expression/case_expression.hpp"

#include "common/exception.hpp"

using namespace duckdb;
using namespace std;

void CaseExpression::ResolveType() {
	Expression::ResolveType();
	ExpressionStatistics::Case(result_if_true->stats, result_if_false->stats, stats);
	return_type = std::max(result_if_true->return_type, result_if_false->return_type);
}

unique_ptr<Expression> CaseExpression::Copy() {
	auto copy = make_unique<CaseExpression>();
	copy->CopyProperties(*this);
	copy->check = check->Copy();
	copy->result_if_true = result_if_true->Copy();
	copy->result_if_false = result_if_false->Copy();
	return copy;
}

void CaseExpression::Serialize(Serializer &serializer) {
	Expression::Serialize(serializer);
	check->Serialize(serializer);
	result_if_true->Serialize(serializer);
	result_if_false->Serialize(serializer);
}

unique_ptr<Expression> CaseExpression::Deserialize(ExpressionType type, TypeId return_type, Deserializer &source) {
	auto expression = make_unique<CaseExpression>();
	expression->return_type = return_type;
	expression->check = Expression::Deserialize(source);
	expression->result_if_true = Expression::Deserialize(source);
	expression->result_if_false = Expression::Deserialize(source);
	return expression;
}

bool CaseExpression::Equals(const Expression *other_) const {
	if (!Expression::Equals(other_)) {
		return false;
	}
	auto other = (CaseExpression *)other_;
	if (!check->Equals(other->check.get())) {
		return false;
	}
	if (!result_if_true->Equals(other->result_if_true.get())) {
		return false;
	}
	if (!result_if_false->Equals(other->result_if_false.get())) {
		return false;
	}
	return true;
}

void CaseExpression::EnumerateChildren(
    std::function<unique_ptr<Expression>(unique_ptr<Expression> expression)> callback) {
	check = callback(move(check));
	result_if_true = callback(move(result_if_true));
	result_if_false = callback(move(result_if_false));
}

void CaseExpression::EnumerateChildren(std::function<void(Expression *expression)> callback) const {
	callback(check.get());
	callback(result_if_true.get());
	callback(result_if_false.get());
}
