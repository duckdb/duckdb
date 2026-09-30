#include "duckdb/optimizer/constraint_propagation/helpers.hpp"

#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"

namespace duckdb {

bool CollectEquiKeyPairs(const FactStore &store, const LogicalComparisonJoin &join,
                         vector<std::pair<idx_t, idx_t>> &pairs, idx_t *matched_conditions) {
	if (matched_conditions) {
		*matched_conditions = 0;
	}
	pairs.clear();
	const auto *b0 = store.FindBindings(join.children[0].get());
	const auto *b1 = store.FindBindings(join.children[1].get());
	if (!b0 || !b1) {
		return false;
	}
	for (auto &cond : join.conditions) {
		if (!cond.IsComparison()) {
			continue;
		}
		bool is_equality = cond.GetComparisonType() == ExpressionType::COMPARE_EQUAL;
		bool is_null_safe = cond.GetComparisonType() == ExpressionType::COMPARE_NOT_DISTINCT_FROM;
		if (!is_equality && !is_null_safe) {
			continue;
		}
		auto &left_expr = cond.LeftReference();
		auto &right_expr = cond.RightReference();
		if (left_expr->GetExpressionType() != ExpressionType::BOUND_COLUMN_REF ||
		    right_expr->GetExpressionType() != ExpressionType::BOUND_COLUMN_REF) {
			continue;
		}
		auto lb = left_expr->Cast<BoundColumnRefExpression>().Binding();
		auto rb = right_expr->Cast<BoundColumnRefExpression>().Binding();
		auto lp = PositionIn(*b0, lb);
		auto rp = PositionIn(*b1, rb);
		if (!lp.IsValid() || !rp.IsValid()) {
			continue;
		}
		if (is_null_safe) {
			const auto &f0 = store.Get(join.children[0].get());
			const auto &f1 = store.Get(join.children[1].get());
			if (!f0.NotNull().Test(lp.GetIndex()) && !f1.NotNull().Test(rp.GetIndex())) {
				continue;
			}
		}
		pairs.emplace_back(lp.GetIndex(), rp.GetIndex());
		if (matched_conditions) {
			(*matched_conditions)++;
		}
	}
	return true;
}

bool CollectEquiKeys(const FactStore &store, const LogicalComparisonJoin &join, ColumnMask &key0, ColumnMask &key1,
                     idx_t *matched_conditions) {
	vector<std::pair<idx_t, idx_t>> pairs;
	if (!CollectEquiKeyPairs(store, join, pairs, matched_conditions)) {
		key0 = key1 = ColumnMask::Empty();
		return false;
	}
	const auto *b0 = store.FindBindings(join.children[0].get());
	const auto *b1 = store.FindBindings(join.children[1].get());
	key0 = ColumnMask(b0->size());
	key1 = ColumnMask(b1->size());
	for (auto &kv : pairs) {
		key0.Set(kv.first);
		key1.Set(kv.second);
	}
	return true;
}

} // namespace duckdb
