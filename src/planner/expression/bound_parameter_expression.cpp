#include "duckdb/planner/expression/bound_parameter_expression.hpp"
#include "duckdb/common/types/hash.hpp"

namespace duckdb {
using namespace std;

BoundParameterExpression::BoundParameterExpression(idx_t parameter_nr)
    : Expression(ExpressionType::VALUE_PARAMETER, ExpressionClass::BOUND_PARAMETER,
                 LogicalType(LogicalTypeId::UNKNOWN)),
      parameter_nr(parameter_nr), value(nullptr) {
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
	if (!Expression::Equals(other_)) {
		return false;
	}
	auto other = (BoundParameterExpression *)other_;
	return parameter_nr == other->parameter_nr;
}

hash_t BoundParameterExpression::Hash() const {
	hash_t result = Expression::Hash();
	result = CombineHash(duckdb::Hash(parameter_nr), result);
	return result;
}

unique_ptr<Expression> BoundParameterExpression::Copy() {
	auto result = make_unique<BoundParameterExpression>(parameter_nr);
	result->value = value;
	result->return_type = return_type;
	return move(result);
}

} // namespace duckdb
