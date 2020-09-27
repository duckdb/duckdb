#include "duckdb/planner/expression/bound_reference_expression.hpp"

#include "duckdb/common/serializer.hpp"
#include "duckdb/common/types/hash.hpp"

namespace duckdb {
using namespace std;

BoundReferenceExpression::BoundReferenceExpression(string alias, LogicalType type, idx_t index)
    : Expression(ExpressionType::BOUND_REF, ExpressionClass::BOUND_REF, move(type)), index(index) {
	this->alias = alias;
}
BoundReferenceExpression::BoundReferenceExpression(LogicalType type, idx_t index)
    : BoundReferenceExpression(string(), move(type), index) {
}

string BoundReferenceExpression::ToString() const {
	return "#" + std::to_string(index);
}

bool BoundReferenceExpression::Equals(const BaseExpression *other_) const {
	if (!Expression::Equals(other_)) {
		return false;
	}
	auto other = (BoundReferenceExpression *)other_;
	return other->index == index;
}

hash_t BoundReferenceExpression::Hash() const {
	return CombineHash(Expression::Hash(), duckdb::Hash<idx_t>(index));
}

unique_ptr<Expression> BoundReferenceExpression::Copy() {
	return make_unique<BoundReferenceExpression>(alias, return_type, index);
}

} // namespace duckdb
