#include "duckdb/optimizer/join_elimination.hpp"

#include "duckdb/common/constants.hpp"
#include "duckdb/common/enums/expression_type.hpp"
#include "duckdb/common/enums/join_type.hpp"
#include "duckdb/common/enums/logical_operator_type.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/logical_operator_visitor.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_distinct.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_empty_result.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/optimizer/constraint_propagation/queries.hpp"

namespace duckdb {

static void CollectExprReferences(Expression &expr, unordered_set<TableIndex> &ref_table_ids) {
	if (expr.GetExpressionType() == ExpressionType::BOUND_COLUMN_REF) {
		ref_table_ids.insert(expr.Cast<BoundColumnRefExpression>().Binding().table_index);
	}
	ExpressionIterator::EnumerateChildren(expr,
	                                      [&](Expression &child) { CollectExprReferences(child, ref_table_ids); });
}

static void CollectReferences(LogicalOperator &op, unordered_set<TableIndex> &ref_table_ids) {
	LogicalOperatorVisitor::EnumerateExpressions(op, [&](const unique_ptr<Expression> *expr_ptr) {
		if (expr_ptr && *expr_ptr) {
			CollectExprReferences(**expr_ptr, ref_table_ids);
		}
	});
}

static bool SideIsUnusedAbove(LogicalComparisonJoin &join, idx_t side, const unordered_set<TableIndex> &ref_table_ids) {
	for (auto &binding : join.children[side]->GetColumnBindings()) {
		if (ref_table_ids.find(binding.table_index) != ref_table_ids.end()) {
			return false;
		}
	}
	return true;
}

static bool TrySelfJoinElimination(LogicalComparisonJoin &join, idx_t keep, idx_t drop,
                                   const ConstraintPropagator &propagator) {
	const auto &keep_facts = propagator.Facts(*join.children[keep]);
	const auto &drop_facts = propagator.Facts(*join.children[drop]);

	if (!keep_facts.base_table || !drop_facts.base_table || keep_facts.base_table != drop_facts.base_table) {
		return false;
	}

	if (drop_facts.filter_below) {
		return false;
	}

	ColumnMask left_keys, right_keys;
	if (!EquiKeys(propagator, join, left_keys, right_keys)) {
		return false;
	}
	const ColumnMask &keep_key = (keep == 0) ? left_keys : right_keys;
	const ColumnMask &drop_key = (drop == 0) ? left_keys : right_keys;
	if (keep_key.IsEmpty()) {
		return false;
	}

	if (!IsUniqueOn(propagator, *join.children[drop], drop_key, false)) {
		return false;
	}

	if (!IsNotNullOn(propagator, *join.children[keep], keep_key)) {
		return false;
	}

	unordered_set<idx_t> keep_phys, drop_phys;
	bool traceable = true;
	keep_key.ForEachPosition([&](idx_t p) -> bool {
		if (p >= keep_facts.base_column.size() || keep_facts.base_column[p] == DConstants::INVALID_INDEX) {
			traceable = false;
			return false;
		}
		keep_phys.insert(keep_facts.base_column[p]);
		return true;
	});
	drop_key.ForEachPosition([&](idx_t p) -> bool {
		if (p >= drop_facts.base_column.size() || drop_facts.base_column[p] == DConstants::INVALID_INDEX) {
			traceable = false;
			return false;
		}
		drop_phys.insert(drop_facts.base_column[p]);
		return true;
	});
	if (!traceable || keep_phys != drop_phys) {
		return false;
	}
	return true;
}

unique_ptr<LogicalOperator> JoinElimination::Optimize(unique_ptr<LogicalOperator> op) {
	// Bounded fixpoint, ONE elimination per iteration.
	for (int iteration = 0; iteration < 10; iteration++) {
		ConstraintPropagator propagator;
		propagator.Analyze(*op);

		bool changed = false;
		unordered_set<TableIndex> ref_table_ids;
		op = OptimizeInternal(std::move(op), std::move(ref_table_ids), false, propagator, changed);
		if (!changed) {
			break;
		}
	}
	return op;
}

unique_ptr<LogicalOperator> JoinElimination::OptimizeInternal(unique_ptr<LogicalOperator> op,
                                                              unordered_set<TableIndex> ref_table_ids,
                                                              bool outer_is_distinct, ConstraintPropagator &propagator,
                                                              bool &changed) {
	unordered_set<TableIndex> refs_above;
	if (op->type == LogicalOperatorType::LOGICAL_FILTER) {
		refs_above = ref_table_ids;
	}

	if (op->type != LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		CollectReferences(*op, ref_table_ids);
	}

	if (op->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		unordered_set<TableIndex> child_ref_table_ids = ref_table_ids;
		CollectReferences(*op, child_ref_table_ids);

		op->children[0] =
		    OptimizeInternal(std::move(op->children[0]), child_ref_table_ids, outer_is_distinct, propagator, changed);
		op->children[1] = OptimizeInternal(std::move(op->children[1]), child_ref_table_ids, false, propagator, changed);

		return TryEliminateJoin(std::move(op), ref_table_ids, outer_is_distinct, propagator, changed);
	}

	// Top-down DISTINCT context
	bool child_is_distinct = outer_is_distinct;
	if (op->type == LogicalOperatorType::LOGICAL_DISTINCT) {
		auto &distinct = op->Cast<LogicalDistinct>();
		child_is_distinct = (distinct.distinct_type == DistinctType::DISTINCT);
	} else if (op->type != LogicalOperatorType::LOGICAL_PROJECTION && op->type != LogicalOperatorType::LOGICAL_FILTER) {
		child_is_distinct = false;
	}

	for (auto &child : op->children) {
		child = OptimizeInternal(std::move(child), ref_table_ids, child_is_distinct, propagator, changed);
	}

	if (op->type == LogicalOperatorType::LOGICAL_FILTER && !changed && op->children.size() == 1 &&
	    op->children[0]->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		auto &join = op->children[0]->Cast<LogicalComparisonJoin>();
		if (join.join_type == JoinType::MARK) {
			return TryEliminateMarkJoin(std::move(op), refs_above, propagator, changed);
		}
	}

	return op;
}

unique_ptr<LogicalOperator> JoinElimination::TryEliminateJoin(unique_ptr<LogicalOperator> op,
                                                              const unordered_set<TableIndex> &ref_table_ids,
                                                              bool outer_is_distinct,
                                                              const ConstraintPropagator &propagator, bool &changed) {
	if (changed) {
		return op;
	}

	auto &join = op->Cast<LogicalComparisonJoin>();

	if (join.filter_pushdown) {
		return op;
	}

	switch (join.join_type) {
	case JoinType::LEFT: {
		if (!SideIsUnusedAbove(join, 1, ref_table_ids)) {
			return op;
		}
		if (outer_is_distinct || MultiplicityOf(propagator, join, 0) >= SideMultiplicity::EXACTLY_ONE) {
			changed = true;
			return std::move(op->children[0]);
		}
		return op;
	}
	case JoinType::RIGHT: {
		if (!SideIsUnusedAbove(join, 0, ref_table_ids)) {
			return op;
		}
		if (outer_is_distinct || MultiplicityOf(propagator, join, 1) >= SideMultiplicity::EXACTLY_ONE) {
			changed = true;
			return std::move(op->children[1]);
		}
		return op;
	}
	case JoinType::INNER: {
		for (idx_t keep = 0; keep < 2; keep++) {
			idx_t drop = 1 - keep;
			if (!SideIsUnusedAbove(join, drop, ref_table_ids)) {
				continue;
			}
			if (MultiplicityOf(propagator, join, keep) >= SideMultiplicity::EXACTLY_ONE ||
			    TrySelfJoinElimination(join, keep, drop, propagator)) {
				changed = true;
				return std::move(op->children[keep]);
			}
		}
		return op;
	}
	case JoinType::SEMI: {
		D_ASSERT(SideIsUnusedAbove(join, 1, ref_table_ids));
		if (JoinCoverage(propagator, join, 0)) {
			changed = true;
			return std::move(op->children[0]);
		}
		return op;
	}
	case JoinType::ANTI: {
		D_ASSERT(SideIsUnusedAbove(join, 1, ref_table_ids));
		if (JoinCoverage(propagator, join, 0)) {
			changed = true;
			return make_uniq<LogicalEmptyResult>(std::move(op->children[0]));
		}
		return op;
	}
	default:
		return op;
	}
}

static optional<bool> MarkSideValue(const Expression &expr, const ColumnBinding &mark_binding) {
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
		auto &ref = expr.Cast<BoundColumnRefExpression>();
		return ref.Binding() == mark_binding ? optional(true) : optional<bool>();
	}
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_OPERATOR &&
	    expr.GetExpressionType() == ExpressionType::OPERATOR_NOT) {
		auto &not_expr = expr.Cast<BoundOperatorExpression>();
		if (not_expr.GetChildren().size() == 1 &&
		    not_expr.GetChildren()[0]->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
			auto &inner = not_expr.GetChildren()[0]->Cast<BoundColumnRefExpression>();
			if (inner.Binding() == mark_binding) {
				return optional(false);
			}
		}
	}
	return optional<bool>();
}

static optional<bool> ClassifyMarkConjunctInner(const Expression &expr, const ColumnBinding &mark_binding) {
	if (auto v = MarkSideValue(expr, mark_binding)) {
		return v;
	}

	if (expr.GetExpressionClass() != ExpressionClass::BOUND_FUNCTION) {
		return optional<bool>();
	}
	bool is_not_distinct;
	switch (expr.GetExpressionType()) {
	case ExpressionType::COMPARE_NOT_DISTINCT_FROM:
		is_not_distinct = true;
		break;
	case ExpressionType::COMPARE_DISTINCT_FROM:
		is_not_distinct = false;
		break;
	default:
		return optional<bool>();
	}

	auto &cmp = expr.Cast<BoundFunctionExpression>();
	if (cmp.GetChildren().size() != 2) {
		return optional<bool>();
	}

	optional<bool> mark_val;
	const Expression *const_side = nullptr;
	if (auto v0 = MarkSideValue(*cmp.GetChildren()[0], mark_binding)) {
		if (cmp.GetChildren()[1]->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
			mark_val = v0;
			const_side = cmp.GetChildren()[1].get();
		}
	}
	if (!mark_val) {
		if (auto v1 = MarkSideValue(*cmp.GetChildren()[1], mark_binding)) {
			if (cmp.GetChildren()[0]->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
				mark_val = v1;
				const_side = cmp.GetChildren()[0].get();
			}
		}
	}
	if (!mark_val || !const_side) {
		return optional<bool>();
	}

	auto &constant = const_side->Cast<BoundConstantExpression>();
	bool const_val;
	if (constant.GetValue() == Value::BOOLEAN(true)) {
		const_val = true;
	} else if (constant.GetValue() == Value::BOOLEAN(false)) {
		const_val = false;
	} else {
		return optional<bool>();
	}

	return is_not_distinct ? (mark_val.value() == const_val) : (mark_val.value() != const_val);
}

static optional<bool> ClassifyMarkConjunct(const Expression &conjunct, const ColumnBinding &mark_binding) {
	if (conjunct.GetExpressionClass() == ExpressionClass::BOUND_OPERATOR &&
	    conjunct.GetExpressionType() == ExpressionType::OPERATOR_NOT) {
		auto &not_expr = conjunct.Cast<BoundOperatorExpression>();
		if (not_expr.GetChildren().size() != 1) {
			return optional<bool>();
		}
		auto inner = ClassifyMarkConjunctInner(*not_expr.GetChildren()[0], mark_binding);
		if (!inner) {
			return optional<bool>();
		}
		return !inner.value();
	}
	return ClassifyMarkConjunctInner(conjunct, mark_binding);
}

unique_ptr<LogicalOperator> JoinElimination::TryEliminateMarkJoin(unique_ptr<LogicalOperator> op,
                                                                  const unordered_set<TableIndex> &refs_above,
                                                                  const ConstraintPropagator &propagator,
                                                                  bool &changed) {
	auto &filter = op->Cast<LogicalFilter>();
	auto &join = filter.children[0]->Cast<LogicalComparisonJoin>();

	if (join.filter_pushdown) {
		return op;
	}

	auto join_bindings = join.GetColumnBindings();
	auto probe_bindings = join.children[0]->GetColumnBindings();
	ColumnBinding mark_binding;
	bool found_mark = false;
	for (auto &b : join_bindings) {
		bool is_probe = false;
		for (auto &pb : probe_bindings) {
			if (pb == b) {
				is_probe = true;
				break;
			}
		}
		if (!is_probe) {
			mark_binding = b;
			found_mark = true;
			break;
		}
	}
	if (!found_mark) {
		return op;
	}

	if (filter.expressions.size() != 1) {
		return op;
	}
	auto &conjunct = filter.expressions[0];

	auto folded = ClassifyMarkConjunct(*conjunct, mark_binding);
	if (!folded) {
		return op;
	}
	bool is_positive = folded.value();

	if (refs_above.find(mark_binding.table_index) != refs_above.end()) {
		return op;
	}

	if (!JoinCoverage(propagator, join, 0)) {
		return op;
	}

	changed = true;
	auto probe = std::move(join.children[0]);
	if (is_positive) {
		return probe;
	}
	return make_uniq<LogicalEmptyResult>(std::move(probe));
}

} // namespace duckdb
