#include "duckdb/planner/expression/bound_constant_expression.hpp"

#include "duckdb/common/types/hash.hpp"
#include "duckdb/common/value_operations/value_operations.hpp"

namespace duckdb {
using namespace std;

BoundConstantExpression::BoundConstantExpression(SQLType return_type, Value value)
    : Expression(ExpressionType::VALUE_CONSTANT, ExpressionClass::BOUND_CONSTANT, value.type, move(return_type)), value(value) {
	assert(value.type == GetInternalType(sql_type));
	this->value.SetSQLType(sql_type);
}

string BoundConstantExpression::ToString() const {
	return value.ToString(sql_type);
}

bool BoundConstantExpression::Equals(const BaseExpression *other_) const {
	if (!BaseExpression::Equals(other_)) {
		return false;
	}
	auto other = (BoundConstantExpression *)other_;
	return value == other->value;
}

hash_t BoundConstantExpression::Hash() const {
	hash_t result = Expression::Hash();
	return CombineHash(ValueOperations::Hash(value), result);
}

unique_ptr<Expression> BoundConstantExpression::Copy() {
	auto copy = make_unique<BoundConstantExpression>(sql_type, value);
	copy->CopyProperties(*this);
	return move(copy);
}

} // namespace duckdb
