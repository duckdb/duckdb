#include "duckdb/planner/expression/bound_lambdaref_expression.hpp"

#include "duckdb/common/types/hash.hpp"
#include "duckdb/common/to_string.hpp"

namespace duckdb {

BoundLambdaRefExpression::BoundLambdaRefExpression(string alias_p, LogicalType type, ColumnBinding binding,
                                                   idx_t lambda_idx, idx_t depth)
    : Expression(ExpressionType::BOUND_LAMBDA_REF, ExpressionClass::BOUND_LAMBDA_REF, std::move(type)),
      binding(binding), lambda_idx(lambda_idx), depth(depth) {
	this->alias = std::move(alias_p);
}

BoundLambdaRefExpression::BoundLambdaRefExpression(LogicalType type, ColumnBinding binding, idx_t lambda_idx,
                                                   idx_t depth)
    : BoundLambdaRefExpression(string(), std::move(type), binding, lambda_idx, depth) {
}

unique_ptr<Expression> BoundLambdaRefExpression::Copy() const {
	return make_uniq<BoundLambdaRefExpression>(alias, return_type, binding, lambda_idx, depth);
}

hash_t BoundLambdaRefExpression::Hash() const {
	auto result = Expression::Hash();
	result = CombineHash(result, duckdb::Hash<uint64_t>(lambda_idx));
	result = CombineHash(result, duckdb::Hash<uint64_t>(binding.column_index));
	result = CombineHash(result, duckdb::Hash<uint64_t>(binding.table_index));
	return CombineHash(result, duckdb::Hash<uint64_t>(depth));
}

bool BoundLambdaRefExpression::Equals(const BaseExpression &other_p) const {
	if (!Expression::Equals(other_p)) {
		return false;
	}
	auto &other = other_p.Cast<BoundLambdaRefExpression>();
	return other.binding == binding && other.lambda_idx == lambda_idx && other.depth == depth;
}

string BoundLambdaRefExpression::ToString() const {
	if (!alias.empty()) {
		return alias;
	}
	return "#[" + to_string(binding.table_index) + "." + to_string(binding.column_index) + "." + to_string(lambda_idx) +
	       "]";
}
} // namespace duckdb
