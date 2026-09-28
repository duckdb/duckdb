#include "duckdb/optimizer/constraint_propagation/helpers.hpp"

#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"

namespace duckdb {

bool CollectEquiKeys(const FactStore &store, const LogicalComparisonJoin &join, ColumnMask &key0, ColumnMask &key1,
                     idx_t *matched_conditions) {
	if (matched_conditions) {
		*matched_conditions = 0;
	}
	const auto *b0 = store.FindBindings(join.children[0].get());
	const auto *b1 = store.FindBindings(join.children[1].get());
	if (!b0 || !b1) {
		key0 = key1 = ColumnMask::Empty();
		return false;
	}
	key0 = ColumnMask(b0->size());
	key1 = ColumnMask(b1->size());
	for (auto &cond : join.conditions) {
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
				continue; // both sides nullable:
			}
		}
		key0.Set(lp.GetIndex());
		key1.Set(rp.GetIndex());
		if (matched_conditions) {
			(*matched_conditions)++;
		}
	}
	return true;
}

} // namespace duckdb
