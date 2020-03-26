#include "duckdb/planner/expression/bound_columnref_expression.hpp"

#include "duckdb/common/types/hash.hpp"

using namespace duckdb;
using namespace std;

BoundColumnRefExpression::BoundColumnRefExpression(string alias, TypeId type, ColumnBinding binding, idx_t depth)
    : Expression(ExpressionType::BOUND_COLUMN_REF, ExpressionClass::BOUND_COLUMN_REF, type), binding(binding),
      depth(depth) {
	this->alias = alias;
}

BoundColumnRefExpression::BoundColumnRefExpression(TypeId type, ColumnBinding binding, idx_t depth)
    : BoundColumnRefExpression(string(), type, binding, depth) {
}

unique_ptr<Expression> BoundColumnRefExpression::Copy() {
	return make_unique<BoundColumnRefExpression>(alias, return_type, binding, depth);
}

hash_t BoundColumnRefExpression::Hash() const {
	auto result = Expression::Hash();
	result = CombineHash(result, duckdb::Hash<uint64_t>(binding.column_index));
	result = CombineHash(result, duckdb::Hash<uint64_t>(binding.table_index));
	return CombineHash(result, duckdb::Hash<uint64_t>(depth));
}

bool BoundColumnRefExpression::Equals(const BaseExpression *other_) const {
	if (!BaseExpression::Equals(other_)) {
		return false;
	}
	auto other = (BoundColumnRefExpression *)other_;
	return other->binding == binding && other->depth == depth;
}

string BoundColumnRefExpression::ToString() const {
	return "#[" + to_string(binding.table_index) + "." + to_string(binding.column_index) + "]";
}
