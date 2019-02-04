#include "parser/expression/case_expression.hpp"
#include "parser/expression/cast_expression.hpp"

#include "common/exception.hpp"

using namespace duckdb;
using namespace std;

void CaseExpression::ResolveType() {
	Expression::ResolveType();
	ExpressionStatistics::Case(result_if_true->stats, result_if_false->stats, stats);
	// CHECK should be a boolean type
	check = CastExpression::AddCastToType(TypeId::BOOLEAN, move(check));
	// use the highest type as the result type of the CASE
	this->return_type = std::max(result_if_true->return_type, result_if_false->return_type);
	// res_if_true and res_if_false need the same type
	result_if_true = CastExpression::AddCastToType(return_type, move(result_if_true));
	result_if_false = CastExpression::AddCastToType(return_type, move(result_if_false));
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

size_t CaseExpression::ChildCount() const {
	return 3;
}

Expression *CaseExpression::GetChild(size_t index) const {
	switch (index) {
	case 0:
		return check.get();
	case 1:
		return result_if_true.get();
	default:
		assert(index == 2);
		return result_if_false.get();
	}
}

void CaseExpression::ReplaceChild(std::function<unique_ptr<Expression>(unique_ptr<Expression> expression)> callback,
                                  size_t index) {
	switch (index) {
	case 0:
		check = callback(move(check));
		break;
	case 1:
		result_if_true = callback(move(result_if_true));
		break;
	default:
		assert(index == 2);
		result_if_false = callback(move(result_if_false));
		break;
	}
}
