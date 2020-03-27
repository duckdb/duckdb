#include "duckdb/planner/expression/bound_parameter_expression.hpp"
#include "duckdb/common/types/hash.hpp"

using namespace duckdb;
using namespace std;

BoundParameterExpression::BoundParameterExpression(idx_t parameter_nr)
    : Expression(ExpressionType::VALUE_PARAMETER, ExpressionClass::BOUND_PARAMETER, TypeId::INVALID),
      sql_type(SQLType(SQLTypeId::UNKNOWN)), parameter_nr(parameter_nr), value(nullptr) {
}

bool BoundParameterExpression::IsScalar() const {
	return true;
}
bool BoundParameterExpression::HasParameter() const {
	return true;
}
bool BoundParameterExpression::IsFoldable() const {
	return false;
}

string BoundParameterExpression::ToString() const {
	return to_string(parameter_nr);
}

bool BoundParameterExpression::Equals(const BaseExpression *other_) const {
	if (!BaseExpression::Equals(other_)) {
		return false;
	}
	auto other = (BoundParameterExpression *)other_;
	return parameter_nr == other->parameter_nr;
}

hash_t BoundParameterExpression::Hash() const {
	hash_t result = Expression::Hash();
	result = CombineHash(duckdb::Hash(parameter_nr), result);
	result = CombineHash(duckdb::Hash((int)sql_type.id), result);
	return result;
}

unique_ptr<Expression> BoundParameterExpression::Copy() {
	auto result = make_unique<BoundParameterExpression>(parameter_nr);
	result->sql_type = sql_type;
	result->value = value;
	result->return_type = return_type;
	return move(result);
}
