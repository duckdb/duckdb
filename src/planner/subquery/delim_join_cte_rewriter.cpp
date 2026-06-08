//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/subquery/delim_join_cte_rewriter.cpp
//
//
//===----------------------------------------------------------------------===//

#include "duckdb/planner/subquery/delim_join_cte_rewriter.hpp"

#include "duckdb/optimizer/column_binding_replacer.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/logical_operator_visitor.hpp"
#include "duckdb/planner/operator/list.hpp"

#include <algorithm>

namespace duckdb {

static void VerifyNoDelim(LogicalOperator &op) {
	// Verify that there are no delim joins or delim scans in the plan, as these should have been rewritten to CTEs at
	// this point.
	if (op.type == LogicalOperatorType::LOGICAL_DELIM_JOIN) {
		throw InternalException("Found DELIM_JOIN after flattening dependent joins");
	}
	if (op.type == LogicalOperatorType::LOGICAL_DELIM_GET) {
		throw InternalException("Found DELIM_GET after flattening dependent joins");
	}
	for (auto &child : op.children) {
		VerifyNoDelim(*child);
	}
}

static vector<string> GenerateCTEColumnNames(idx_t column_count, const string &prefix) {
	vector<string> result;
	result.reserve(column_count);
	for (idx_t i = 0; i < column_count; i++) {
		result.push_back(prefix + to_string(i));
	}
	return result;
}

static idx_t RewriteDelimScanReferences(unique_ptr<LogicalOperator> &op, TableIndex delim_scan_index) {
	if (op->type == LogicalOperatorType::LOGICAL_DELIM_JOIN) {
		if (!op->children.empty()) {
			return RewriteDelimScanReferences(op->children[0], delim_scan_index);
		}
		return 0;
	}
	idx_t rewritten_count = 0;
	for (auto &child : op->children) {
		rewritten_count += RewriteDelimScanReferences(child, delim_scan_index);
	}
	if (op->type == LogicalOperatorType::LOGICAL_DELIM_GET) {
		auto &delim_get = op->Cast<LogicalDelimGet>();
		auto delim_scan_names = GenerateCTEColumnNames(delim_get.chunk_types.size(), "__duckdb_delim_scan_");
		auto cte_scan =
		    make_uniq<LogicalCTERef>(delim_get.table_index, delim_scan_index, delim_get.chunk_types, delim_scan_names);
		op = std::move(cte_scan);
		rewritten_count++;
	}
	return rewritten_count;
}

static vector<ReplacementBinding> CreateBindingReplacements(const vector<ColumnBinding> &old_bindings,
                                                            const vector<ColumnBinding> &new_bindings) {
	vector<ReplacementBinding> result;
	auto count = MinValue(old_bindings.size(), new_bindings.size());
	for (idx_t i = 0; i < count; i++) {
		if (old_bindings[i] != new_bindings[i] &&
		    std::find(new_bindings.begin(), new_bindings.end(), old_bindings[i]) == new_bindings.end()) {
			result.emplace_back(old_bindings[i], new_bindings[i]);
		}
	}
	return result;
}

static vector<ReplacementBinding> RewriteChangedChildBindings(LogicalOperator &op, LogicalOperator &child,
                                                              const vector<ColumnBinding> &old_child_bindings) {
	auto new_child_bindings = child.GetColumnBindings();
	auto replacements = CreateBindingReplacements(old_child_bindings, new_child_bindings);
	if (replacements.empty()) {
		return replacements;
	}
	if (op.type == LogicalOperatorType::LOGICAL_DEPENDENT_JOIN) {
		auto &dependent_join = op.Cast<LogicalDependentJoin>();
		for (auto &col : dependent_join.correlated_columns) {
			for (const auto &replacement : replacements) {
				if (col.binding == replacement.old_binding) {
					col.binding = replacement.new_binding;
					break;
				}
			}
		}
	}
	ColumnBindingReplacer replacer;
	replacer.replacement_bindings = replacements;
	if (op.type == LogicalOperatorType::LOGICAL_DEPENDENT_JOIN && op.children[0].get() == &child) {
		replacer.stop_operator = child;
		replacer.VisitOperator(op);
	} else {
		LogicalOperatorVisitor::EnumerateExpressions(
		    op, [&](unique_ptr<Expression> *expr) { replacer.VisitExpression(expr); });
	}
	return replacements;
}

static optional_idx FindBindingIndex(const vector<ColumnBinding> &bindings, const ColumnBinding &binding) {
	auto entry = std::find(bindings.begin(), bindings.end(), binding);
	if (entry == bindings.end()) {
		return optional_idx();
	}
	return NumericCast<idx_t>(entry - bindings.begin());
}

static void AddFilterToOperator(unique_ptr<LogicalOperator> &child, unique_ptr<Expression> filter) {
	if (child->type == LogicalOperatorType::LOGICAL_FILTER && !child->HasProjectionMap()) {
		child->Cast<LogicalFilter>().expressions.push_back(std::move(filter));
		return;
	}

	auto new_filter = make_uniq<LogicalFilter>();
	new_filter->expressions.push_back(std::move(filter));
	new_filter->children.push_back(std::move(child));
	child = std::move(new_filter);
}

static bool GetExpressionColumnBindings(Expression &expr, column_binding_set_t &bindings) {
	bool depth_zero = true;
	ExpressionIterator::VisitExpression<BoundColumnRefExpression>(expr, [&](const BoundColumnRefExpression &colref) {
		if (colref.Depth() == 0) {
			bindings.insert(colref.Binding());
		} else {
			depth_zero = false;
		}
	});
	return depth_zero;
}

static bool ChildContainsBindings(LogicalOperator &child, const column_binding_set_t &bindings) {
	column_binding_set_t child_bindings;
	for (auto &binding : child.GetColumnBindings()) {
		child_bindings.insert(binding);
	}
	for (auto &binding : bindings) {
		if (child_bindings.find(binding) == child_bindings.end()) {
			return false;
		}
	}
	return true;
}

static bool FilterReferencesDelimInput(LogicalComparisonJoin &delim_join, Expression &filter) {
	D_ASSERT(delim_join.type == LogicalOperatorType::LOGICAL_DELIM_JOIN);
	column_binding_set_t filter_bindings;
	if (!GetExpressionColumnBindings(filter, filter_bindings)) {
		return false;
	}
	if (filter_bindings.empty()) {
		return false;
	}
	return ChildContainsBindings(*delim_join.children[0], filter_bindings);
}

static bool PushEligibleFilterExpressionsIntoDelimJoinInputs(unique_ptr<LogicalOperator> &plan) {
	auto &filter = plan->Cast<LogicalFilter>();
	if (filter.HasProjectionMap()) {
		return false;
	}
	if (filter.children[0]->type != LogicalOperatorType::LOGICAL_DELIM_JOIN) {
		return false;
	}

	bool changed = false;
	auto &delim_join = filter.children[0]->Cast<LogicalComparisonJoin>();
	vector<unique_ptr<Expression>> remaining_expressions;
	auto expressions = std::move(filter.expressions);
	LogicalFilter::SplitPredicates(expressions);
	for (auto &expr : expressions) {
		if (FilterReferencesDelimInput(delim_join, *expr)) {
			AddFilterToOperator(delim_join.children[0], std::move(expr));
			changed = true;
			continue;
		}
		remaining_expressions.push_back(std::move(expr));
	}

	if (remaining_expressions.empty()) {
		plan = std::move(filter.children[0]);
	} else {
		filter.expressions = std::move(remaining_expressions);
	}
	return changed;
}

static bool PushEligibleFiltersIntoDelimJoinInputs(unique_ptr<LogicalOperator> &plan) {
	bool changed = false;
	for (auto &child : plan->children) {
		changed = PushEligibleFiltersIntoDelimJoinInputs(child) || changed;
	}
	if (plan->type == LogicalOperatorType::LOGICAL_FILTER) {
		changed = PushEligibleFilterExpressionsIntoDelimJoinInputs(plan) || changed;
	}
	return changed;
}

struct JoinWithGeneratedDedupRef {
	JoinWithGeneratedDedupRef(unique_ptr<LogicalOperator> &join_p, idx_t depth_p, bool filter_cross_product_p = false)
	    : join(join_p), depth(depth_p), filter_cross_product(filter_cross_product_p) {
	}

	reference<unique_ptr<LogicalOperator>> join;
	idx_t depth;
	bool filter_cross_product;
};

static bool IsEqualityJoinCondition(const JoinCondition &cond) {
	if (!cond.IsComparison()) {
		return false;
	}
	switch (cond.GetComparisonType()) {
	case ExpressionType::COMPARE_EQUAL:
	case ExpressionType::COMPARE_NOT_DISTINCT_FROM:
		return true;
	default:
		return false;
	}
}

static bool ChildJoinTypeCanBeDeliminated(JoinType join_type) {
	switch (join_type) {
	case JoinType::INNER:
	case JoinType::SEMI:
		return true;
	default:
		return false;
	}
}

static bool InequalityDelimJoinCanBeEliminated(JoinType join_type) {
	return join_type == JoinType::ANTI || join_type == JoinType::MARK || join_type == JoinType::SEMI ||
	       join_type == JoinType::SINGLE;
}

static bool FindAndReplaceBindings(vector<ColumnBinding> &traced_bindings,
                                   const vector<unique_ptr<Expression>> &expressions,
                                   const vector<ColumnBinding> &current_bindings) {
	for (auto &binding : traced_bindings) {
		idx_t current_idx;
		for (current_idx = 0; current_idx < expressions.size(); current_idx++) {
			if (binding == current_bindings[current_idx]) {
				break;
			}
		}

		if (current_idx == expressions.size() ||
		    expressions[current_idx]->GetExpressionType() != ExpressionType::BOUND_COLUMN_REF) {
			return false;
		}

		auto &colref = expressions[current_idx]->Cast<BoundColumnRefExpression>();
		binding = colref.Binding();
	}
	return true;
}

static optional_ptr<LogicalCTERef> GetGeneratedDedupRef(LogicalOperator &op, TableIndex dedup_cte_index) {
	optional_ptr<LogicalCTERef> result;
	if (op.type == LogicalOperatorType::LOGICAL_CTE_REF) {
		auto &cteref = op.Cast<LogicalCTERef>();
		if (cteref.cte_index == dedup_cte_index) {
			result = cteref;
		}
		return result;
	}
	if (op.type == LogicalOperatorType::LOGICAL_FILTER) {
		auto &filter = op.Cast<LogicalFilter>();
		if (!filter.HasProjectionMap() && filter.children.size() == 1 &&
		    filter.children[0]->type == LogicalOperatorType::LOGICAL_CTE_REF) {
			auto &cteref = filter.children[0]->Cast<LogicalCTERef>();
			if (cteref.cte_index == dedup_cte_index) {
				result = cteref;
			}
		}
	}
	return result;
}

static bool OperatorIsGeneratedDedupRef(LogicalOperator &op, TableIndex dedup_cte_index) {
	return GetGeneratedDedupRef(op, dedup_cte_index) != nullptr;
}

static bool FilterIsGeneratedDedupCrossProduct(LogicalOperator &op, TableIndex dedup_cte_index) {
	if (op.type != LogicalOperatorType::LOGICAL_FILTER || op.HasProjectionMap()) {
		return false;
	}
	auto &filter = op.Cast<LogicalFilter>();
	if (filter.children.size() != 1 || filter.children[0]->type != LogicalOperatorType::LOGICAL_CROSS_PRODUCT) {
		return false;
	}
	auto &cross_product = *filter.children[0];
	return cross_product.children.size() == 2 &&
	       (OperatorIsGeneratedDedupRef(*cross_product.children[0], dedup_cte_index) ||
	        OperatorIsGeneratedDedupRef(*cross_product.children[1], dedup_cte_index));
}

static void FindJoinsWithGeneratedDedupRefs(unique_ptr<LogicalOperator> &op, TableIndex dedup_cte_index,
                                            vector<JoinWithGeneratedDedupRef> &joins, idx_t depth = 0) {
	if (op->type == LogicalOperatorType::LOGICAL_DELIM_JOIN) {
		if (!op->children.empty()) {
			FindJoinsWithGeneratedDedupRefs(op->children[0], dedup_cte_index, joins, depth + 1);
		}
		return;
	}

	for (auto &child : op->children) {
		FindJoinsWithGeneratedDedupRefs(child, dedup_cte_index, joins, depth);
	}

	if (op->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN &&
	    (OperatorIsGeneratedDedupRef(*op->children[0], dedup_cte_index) ||
	     OperatorIsGeneratedDedupRef(*op->children[1], dedup_cte_index))) {
		joins.emplace_back(op, depth);
	} else if (FilterIsGeneratedDedupCrossProduct(*op, dedup_cte_index)) {
		joins.emplace_back(op, depth, true);
	}
}

static idx_t CountGeneratedDedupRefs(LogicalOperator &op, TableIndex dedup_cte_index) {
	if (op.type == LogicalOperatorType::LOGICAL_DELIM_JOIN) {
		if (!op.children.empty()) {
			return CountGeneratedDedupRefs(*op.children[0], dedup_cte_index);
		}
		return 0;
	}

	idx_t count = 0;
	if (op.type == LogicalOperatorType::LOGICAL_CTE_REF && op.Cast<LogicalCTERef>().cte_index == dedup_cte_index) {
		count++;
	}
	for (auto &child : op.children) {
		count += CountGeneratedDedupRefs(*child, dedup_cte_index);
	}
	return count;
}

static bool ExpressionReferencesTable(Expression &expr, TableIndex table_index) {
	bool found = false;
	ExpressionIterator::VisitExpression<BoundColumnRefExpression>(expr, [&](const BoundColumnRefExpression &colref) {
		if (colref.Depth() == 0 && colref.Binding().table_index == table_index) {
			found = true;
		}
	});
	return found;
}

static bool RemoveInequalityJoinConditionsWithGeneratedDedupRef(LogicalComparisonJoin &delim_join,
                                                                idx_t dedup_ref_count, LogicalOperator &target_op,
                                                                const vector<JoinCondition> &join_conditions,
                                                                idx_t dedup_idx) {
	auto &delim_conditions = delim_join.conditions;
	if (dedup_ref_count != 1 || !InequalityDelimJoinCanBeEliminated(delim_join.join_type) ||
	    delim_conditions.size() != join_conditions.size()) {
		return false;
	}

	if (delim_join.join_type == JoinType::SINGLE || delim_join.join_type == JoinType::MARK) {
		bool has_one_equality = false;
		for (auto &cond : join_conditions) {
			has_one_equality = has_one_equality || IsEqualityJoinCondition(cond);
		}
		if (!has_one_equality) {
			return false;
		}
	}

	vector<ColumnBinding> traced_bindings;
	for (const auto &cond : delim_conditions) {
		if (!cond.IsComparison() || cond.GetRHS().GetExpressionType() != ExpressionType::BOUND_COLUMN_REF) {
			return false;
		}
		auto &colref = cond.GetRHS().Cast<BoundColumnRefExpression>();
		traced_bindings.emplace_back(colref.Binding());
	}

	reference<LogicalOperator> current_op = *delim_join.children[1];
	while (&current_op.get() != &target_op) {
		if (current_op.get().children.size() != 1) {
			return false;
		}

		switch (current_op.get().type) {
		case LogicalOperatorType::LOGICAL_PROJECTION:
			if (!FindAndReplaceBindings(traced_bindings, current_op.get().expressions,
			                            current_op.get().GetColumnBindings())) {
				return false;
			}
			break;
		case LogicalOperatorType::LOGICAL_FILTER:
			break;
		default:
			return false;
		}
		current_op = *current_op.get().children[0];
	}

	bool found_all = true;
	for (idx_t cond_idx = 0; cond_idx < delim_conditions.size(); cond_idx++) {
		auto &delim_condition = delim_conditions[cond_idx];
		if (!delim_condition.IsComparison()) {
			continue;
		}
		const auto &traced_binding = traced_bindings[cond_idx];

		bool found = false;
		for (auto &join_condition : join_conditions) {
			if (!join_condition.IsComparison()) {
				continue;
			}
			auto &dedup_side = dedup_idx == 0 ? join_condition.GetLHS() : join_condition.GetRHS();
			if (dedup_side.GetExpressionType() != ExpressionType::BOUND_COLUMN_REF) {
				continue;
			}
			auto &colref = dedup_side.Cast<BoundColumnRefExpression>();
			if (colref.Binding() == traced_binding) {
				auto join_comparison = join_condition.GetComparisonType();
				auto original_join_comparison = join_condition.GetComparisonType();
				if (delim_condition.GetComparisonType() == ExpressionType::COMPARE_DISTINCT_FROM ||
				    delim_condition.GetComparisonType() == ExpressionType::COMPARE_NOT_DISTINCT_FROM) {
					if (join_comparison == ExpressionType::COMPARE_EQUAL) {
						join_comparison = ExpressionType::COMPARE_NOT_DISTINCT_FROM;
					} else if (join_comparison == ExpressionType::COMPARE_NOTEQUAL) {
						join_comparison = ExpressionType::COMPARE_DISTINCT_FROM;
					} else if (join_comparison != ExpressionType::COMPARE_DISTINCT_FROM &&
					           join_comparison != ExpressionType::COMPARE_NOT_DISTINCT_FROM) {
						found = false;
						break;
					}
				}
				auto left_copy = delim_condition.LeftReference()->Copy();
				auto right_copy = delim_condition.RightReference()->Copy();

				delim_conditions[cond_idx] = JoinCondition(std::move(left_copy), std::move(right_copy),
				                                           FlipComparisonExpression(join_comparison));
				if (delim_join.join_type != JoinType::MARK &&
				    original_join_comparison != ExpressionType::COMPARE_DISTINCT_FROM &&
				    original_join_comparison != ExpressionType::COMPARE_NOT_DISTINCT_FROM) {
					auto final_comparison = delim_conditions[cond_idx].GetComparisonType();
					if (final_comparison == ExpressionType::COMPARE_DISTINCT_FROM) {
						final_comparison = ExpressionType::COMPARE_NOTEQUAL;
					} else if (final_comparison == ExpressionType::COMPARE_NOT_DISTINCT_FROM) {
						final_comparison = ExpressionType::COMPARE_EQUAL;
					}
					delim_conditions[cond_idx] =
					    JoinCondition(delim_conditions[cond_idx].LeftReference()->Copy(),
					                  delim_conditions[cond_idx].RightReference()->Copy(), final_comparison);
				}
				found = true;
				break;
			}
		}
		found_all = found_all && found;
	}

	return found_all;
}

static bool RemoveInequalityJoinWithGeneratedDedupRef(LogicalComparisonJoin &delim_join, idx_t dedup_ref_count,
                                                      unique_ptr<LogicalOperator> &join, TableIndex dedup_cte_index) {
	auto &comparison_join = join->Cast<LogicalComparisonJoin>();
	const idx_t dedup_idx = OperatorIsGeneratedDedupRef(*join->children[0], dedup_cte_index) ? 0 : 1;
	return RemoveInequalityJoinConditionsWithGeneratedDedupRef(delim_join, dedup_ref_count, *join,
	                                                           comparison_join.conditions, dedup_idx);
}

static bool RemoveJoinWithGeneratedDedupRef(LogicalComparisonJoin &delim_join, idx_t dedup_ref_count,
                                            unique_ptr<LogicalOperator> &join, TableIndex dedup_cte_index,
                                            LogicalOperator &rewrite_root) {
	auto &comparison_join = join->Cast<LogicalComparisonJoin>();
	if (!ChildJoinTypeCanBeDeliminated(comparison_join.join_type)) {
		return false;
	}

	const idx_t dedup_idx = OperatorIsGeneratedDedupRef(*join->children[0], dedup_cte_index) ? 0 : 1;

	optional_ptr<LogicalFilter> filter;
	vector<unique_ptr<Expression>> filter_expressions;
	if (join->children[dedup_idx]->type == LogicalOperatorType::LOGICAL_FILTER) {
		filter = &join->children[dedup_idx]->Cast<LogicalFilter>();
		for (auto &expr : filter->expressions) {
			filter_expressions.emplace_back(expr->Copy());
		}
	}

	auto dedup_ref = GetGeneratedDedupRef(*join->children[dedup_idx], dedup_cte_index);
	if (!dedup_ref) {
		return false;
	}
	if (comparison_join.conditions.size() != dedup_ref->chunk_types.size()) {
		return false;
	}

	ColumnBindingReplacer replacer;
	auto &replacement_bindings = replacer.replacement_bindings;
	bool all_equality_conditions = true;
	for (auto &cond : comparison_join.conditions) {
		if (!cond.IsComparison()) {
			return false;
		}
		all_equality_conditions = all_equality_conditions && IsEqualityJoinCondition(cond);
		auto &dedup_side = dedup_idx == 0 ? cond.GetLHS() : cond.GetRHS();
		auto &other_side = dedup_idx == 0 ? cond.GetRHS() : cond.GetLHS();
		if (dedup_side.GetExpressionType() != ExpressionType::BOUND_COLUMN_REF ||
		    other_side.GetExpressionType() != ExpressionType::BOUND_COLUMN_REF) {
			return false;
		}
		auto &dedup_colref = dedup_side.Cast<BoundColumnRefExpression>();
		auto &other_colref = other_side.Cast<BoundColumnRefExpression>();
		if (dedup_colref.Binding().table_index != dedup_ref->table_index) {
			return false;
		}
		replacement_bindings.emplace_back(dedup_colref.Binding(), other_colref.Binding());

		if (cond.GetComparisonType() != ExpressionType::COMPARE_NOT_DISTINCT_FROM &&
		    cond.GetComparisonType() != ExpressionType::COMPARE_DISTINCT_FROM) {
			auto is_not_null_expr =
			    make_uniq<BoundOperatorExpression>(ExpressionType::OPERATOR_IS_NOT_NULL, LogicalType::BOOLEAN);
			is_not_null_expr->GetChildrenMutable().push_back(other_side.Copy());
			filter_expressions.push_back(std::move(is_not_null_expr));
		}
	}

	if (!all_equality_conditions &&
	    !RemoveInequalityJoinWithGeneratedDedupRef(delim_join, dedup_ref_count, join, dedup_cte_index)) {
		return false;
	}

	unique_ptr<LogicalOperator> replacement_op = std::move(comparison_join.children[1 - dedup_idx]);
	if (!filter_expressions.empty()) {
		auto new_filter = make_uniq<LogicalFilter>();
		new_filter->expressions = std::move(filter_expressions);
		new_filter->children.emplace_back(std::move(replacement_op));
		replacement_op = std::move(new_filter);
	}

	join = std::move(replacement_op);

	replacer.VisitOperator(rewrite_root);
	return true;
}

static bool RemoveFilterCrossProductWithGeneratedDedupRef(LogicalComparisonJoin &delim_join, idx_t dedup_ref_count,
                                                          unique_ptr<LogicalOperator> &filter_op,
                                                          TableIndex dedup_cte_index, LogicalOperator &rewrite_root) {
	auto &filter = filter_op->Cast<LogicalFilter>();
	D_ASSERT(filter.children.size() == 1);
	auto &cross_product = *filter.children[0];
	D_ASSERT(cross_product.type == LogicalOperatorType::LOGICAL_CROSS_PRODUCT);

	const idx_t dedup_idx = OperatorIsGeneratedDedupRef(*cross_product.children[0], dedup_cte_index) ? 0 : 1;
	auto dedup_ref = GetGeneratedDedupRef(*cross_product.children[dedup_idx], dedup_cte_index);
	if (!dedup_ref) {
		return false;
	}

	vector<unique_ptr<Expression>> generated_filter_expressions;
	if (cross_product.children[dedup_idx]->type == LogicalOperatorType::LOGICAL_FILTER) {
		auto &dedup_filter = cross_product.children[dedup_idx]->Cast<LogicalFilter>();
		for (auto &expr : dedup_filter.expressions) {
			generated_filter_expressions.emplace_back(expr->Copy());
		}
	}

	filter.SplitPredicates();
	vector<bool> consumed(filter.expressions.size(), false);
	vector<JoinCondition> join_conditions;
	ColumnBindingReplacer replacer;
	auto &replacement_bindings = replacer.replacement_bindings;
	bool all_equality_conditions = true;

	for (idx_t expr_idx = 0; expr_idx < filter.expressions.size(); expr_idx++) {
		auto &expr = *filter.expressions[expr_idx];
		if (!BoundComparisonExpression::IsComparison(expr)) {
			continue;
		}
		auto &comparison = expr.Cast<BoundFunctionExpression>();
		auto &lhs = BoundComparisonExpression::Left(comparison);
		auto &rhs = BoundComparisonExpression::Right(comparison);
		if (lhs.GetExpressionType() != ExpressionType::BOUND_COLUMN_REF ||
		    rhs.GetExpressionType() != ExpressionType::BOUND_COLUMN_REF) {
			continue;
		}

		auto &lhs_colref = lhs.Cast<BoundColumnRefExpression>();
		auto &rhs_colref = rhs.Cast<BoundColumnRefExpression>();
		auto lhs_dedup = lhs_colref.Binding().table_index == dedup_ref->table_index;
		auto rhs_dedup = rhs_colref.Binding().table_index == dedup_ref->table_index;
		if (lhs_dedup == rhs_dedup) {
			continue;
		}

		auto comparison_type = expr.GetExpressionType();
		if (lhs_dedup) {
			replacement_bindings.emplace_back(lhs_colref.Binding(), rhs_colref.Binding());
			if (dedup_idx == 0) {
				join_conditions.emplace_back(lhs.Copy(), rhs.Copy(), comparison_type);
			} else {
				join_conditions.emplace_back(rhs.Copy(), lhs.Copy(), FlipComparisonExpression(comparison_type));
			}
			if (comparison_type != ExpressionType::COMPARE_NOT_DISTINCT_FROM &&
			    comparison_type != ExpressionType::COMPARE_DISTINCT_FROM) {
				auto is_not_null_expr =
				    make_uniq<BoundOperatorExpression>(ExpressionType::OPERATOR_IS_NOT_NULL, LogicalType::BOOLEAN);
				is_not_null_expr->GetChildrenMutable().push_back(rhs.Copy());
				generated_filter_expressions.push_back(std::move(is_not_null_expr));
			}
		} else {
			replacement_bindings.emplace_back(rhs_colref.Binding(), lhs_colref.Binding());
			if (dedup_idx == 0) {
				join_conditions.emplace_back(rhs.Copy(), lhs.Copy(), FlipComparisonExpression(comparison_type));
			} else {
				join_conditions.emplace_back(lhs.Copy(), rhs.Copy(), comparison_type);
			}
			if (comparison_type != ExpressionType::COMPARE_NOT_DISTINCT_FROM &&
			    comparison_type != ExpressionType::COMPARE_DISTINCT_FROM) {
				auto is_not_null_expr =
				    make_uniq<BoundOperatorExpression>(ExpressionType::OPERATOR_IS_NOT_NULL, LogicalType::BOOLEAN);
				is_not_null_expr->GetChildrenMutable().push_back(lhs.Copy());
				generated_filter_expressions.push_back(std::move(is_not_null_expr));
			}
		}
		all_equality_conditions = all_equality_conditions && IsEqualityJoinCondition(join_conditions.back());
		consumed[expr_idx] = true;
	}

	if (join_conditions.size() != dedup_ref->chunk_types.size()) {
		return false;
	}
	for (idx_t expr_idx = 0; expr_idx < filter.expressions.size(); expr_idx++) {
		if (!consumed[expr_idx] && ExpressionReferencesTable(*filter.expressions[expr_idx], dedup_ref->table_index)) {
			return false;
		}
	}

	if (!all_equality_conditions && !RemoveInequalityJoinConditionsWithGeneratedDedupRef(
	                                    delim_join, dedup_ref_count, *filter_op, join_conditions, dedup_idx)) {
		return false;
	}

	unique_ptr<LogicalOperator> replacement_op = std::move(cross_product.children[1 - dedup_idx]);
	for (idx_t expr_idx = 0; expr_idx < filter.expressions.size(); expr_idx++) {
		if (!consumed[expr_idx]) {
			generated_filter_expressions.push_back(std::move(filter.expressions[expr_idx]));
		}
	}
	if (!generated_filter_expressions.empty()) {
		auto new_filter = make_uniq<LogicalFilter>();
		new_filter->expressions = std::move(generated_filter_expressions);
		new_filter->children.emplace_back(std::move(replacement_op));
		replacement_op = std::move(new_filter);
	}

	filter_op = std::move(replacement_op);

	replacer.VisitOperator(rewrite_root);
	return true;
}

static idx_t RemoveRedundantGeneratedDedupRefs(LogicalComparisonJoin &delim_join, TableIndex dedup_cte_index,
                                               idx_t dedup_ref_count, LogicalOperator &rewrite_root) {
	vector<JoinWithGeneratedDedupRef> joins;
	FindJoinsWithGeneratedDedupRefs(delim_join.children[1], dedup_cte_index, joins);
	if (joins.empty()) {
		return dedup_ref_count;
	}

	std::sort(joins.begin(), joins.end(),
	          [](const JoinWithGeneratedDedupRef &lhs, const JoinWithGeneratedDedupRef &rhs) {
		          return lhs.depth > rhs.depth;
	          });

	for (auto &join : joins) {
		if (join.filter_cross_product) {
			RemoveFilterCrossProductWithGeneratedDedupRef(delim_join, dedup_ref_count, join.join.get(), dedup_cte_index,
			                                              rewrite_root);
		} else {
			RemoveJoinWithGeneratedDedupRef(delim_join, dedup_ref_count, join.join.get(), dedup_cte_index,
			                                rewrite_root);
		}
	}
	return CountGeneratedDedupRefs(*delim_join.children[1], dedup_cte_index);
}

static void TrySwitchSingleToLeft(LogicalComparisonJoin &delim_join) {
	if (delim_join.join_type != JoinType::SINGLE) {
		return;
	}

	vector<ColumnBinding> join_bindings;
	for (const auto &cond : delim_join.conditions) {
		if (!IsEqualityJoinCondition(cond)) {
			return;
		}
		if (!cond.IsComparison() || cond.GetRHS().GetExpressionType() != ExpressionType::BOUND_COLUMN_REF) {
			return;
		}
		auto &colref = cond.GetRHS().Cast<BoundColumnRefExpression>();
		join_bindings.emplace_back(colref.Binding());
	}

	reference<LogicalOperator> current_op = *delim_join.children[1];
	while (current_op.get().type != LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
		if (current_op.get().children.size() != 1) {
			return;
		}

		switch (current_op.get().type) {
		case LogicalOperatorType::LOGICAL_PROJECTION:
			if (!FindAndReplaceBindings(join_bindings, current_op.get().expressions,
			                            current_op.get().GetColumnBindings())) {
				return;
			}
			break;
		case LogicalOperatorType::LOGICAL_FILTER:
			break;
		default:
			return;
		}
		current_op = *current_op.get().children[0];
	}

	auto &aggr = current_op.get().Cast<LogicalAggregate>();
	if (!aggr.grouping_functions.empty()) {
		return;
	}

	for (idx_t group_idx = 0; group_idx < aggr.groups.size(); group_idx++) {
		if (std::find(join_bindings.begin(), join_bindings.end(),
		              ColumnBinding(aggr.group_index, ProjectionIndex(group_idx))) == join_bindings.end()) {
			return;
		}
	}

	delim_join.join_type = JoinType::LEFT;
}

DelimJoinCTERewriter::DelimJoinCTERewriter(Binder &binder) : binder(binder) {
}

void DelimJoinCTERewriter::MaterializeDelimJoinAsCTE(unique_ptr<LogicalOperator> &plan) {
	auto &join = plan->Cast<LogicalComparisonJoin>();
	if (join.delim_flipped) {
		throw InternalException("Flatten dependent joins - flipped delim join CTE rewrite not supported");
	}

	plan->type = LogicalOperatorType::LOGICAL_COMPARISON_JOIN;
	if (join.join_type == JoinType::MARK) {
		// Match the LOGICAL_DELIM_JOIN filter-pushdown semantics: the mark column can still be required above
		// this join, so pushing NOT(mark) into the join must not drop it by rewriting to ANTI.
		join.convert_mark_to_semi = false;
	}

	auto dedup_cte_index = binder.GenerateTableIndex();
	auto dedup_ref_count = RewriteDelimScanReferences(plan->children[1], dedup_cte_index);
	dedup_ref_count = RemoveRedundantGeneratedDedupRefs(join, dedup_cte_index, dedup_ref_count, *plan);
	TrySwitchSingleToLeft(join);
	if (dedup_ref_count == 0) {
		join.duplicate_eliminated_columns.clear();
		return;
	}

	plan->children[0]->ResolveOperatorTypes();
	auto left_bindings = plan->children[0]->GetColumnBindings();
	auto left_types = plan->children[0]->types;
	auto visible_left_column_count = left_bindings.size();

	vector<idx_t> dedup_column_indices;
	vector<LogicalType> dedup_types;
	vector<string> dedup_names;
	vector<unique_ptr<Expression>> extra_left_expressions;
	dedup_column_indices.reserve(join.duplicate_eliminated_columns.size());
	dedup_types.reserve(join.duplicate_eliminated_columns.size());
	dedup_names.reserve(join.duplicate_eliminated_columns.size());
	for (auto &expr : join.duplicate_eliminated_columns) {
		optional_idx binding_index;
		if (expr->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
			auto &colref_expr = expr->Cast<BoundColumnRefExpression>();
			binding_index = FindBindingIndex(left_bindings, colref_expr.Binding());
		}
		if (binding_index.IsValid()) {
			dedup_column_indices.push_back(binding_index.GetIndex());
		} else {
			dedup_column_indices.push_back(left_bindings.size() + extra_left_expressions.size());
			extra_left_expressions.push_back(expr->Copy());
		}
		dedup_types.push_back(expr->GetReturnType());
		dedup_names.push_back(expr->GetName());
	}

	if (!extra_left_expressions.empty()) {
		vector<unique_ptr<Expression>> expressions;
		expressions.reserve(left_bindings.size() + extra_left_expressions.size());
		for (idx_t i = 0; i < left_bindings.size(); i++) {
			expressions.push_back(make_uniq<BoundColumnRefExpression>(left_types[i], left_bindings[i]));
		}
		for (auto &expr : extra_left_expressions) {
			expressions.push_back(std::move(expr));
		}
		auto projection = make_uniq<LogicalProjection>(binder.GenerateTableIndex(), std::move(expressions));
		projection->children.push_back(std::move(plan->children[0]));
		plan->children[0] = std::move(projection);
		plan->children[0]->ResolveOperatorTypes();
		left_bindings = plan->children[0]->GetColumnBindings();
		left_types = plan->children[0]->types;
		if (join.left_projection_map.empty()) {
			join.left_projection_map.reserve(visible_left_column_count);
			for (idx_t i = 0; i < visible_left_column_count; i++) {
				join.left_projection_map.emplace_back(i);
			}
		}
	}

	auto left_column_count = left_bindings.size();
	auto cte_source_bindings = left_bindings;
	vector<unique_ptr<Expression>> cte_source_expressions;
	cte_source_expressions.reserve(left_column_count);
	for (idx_t i = 0; i < left_column_count; i++) {
		cte_source_expressions.push_back(make_uniq<BoundColumnRefExpression>(left_types[i], left_bindings[i]));
	}
	auto cte_source = make_uniq<LogicalProjection>(binder.GenerateTableIndex(), std::move(cte_source_expressions));
	cte_source->children.push_back(std::move(plan->children[0]));
	cte_source->ResolveOperatorTypes();
	left_types = cte_source->types;

	auto cte_index = binder.GenerateTableIndex();
	auto cte_name = "__duckdb_delim_" + to_string(cte_index.index);

	auto left_cte_ref_index = binder.GenerateTableIndex();
	auto left_cte_ref = make_uniq<LogicalCTERef>(left_cte_ref_index, cte_index, left_types,
	                                             GenerateCTEColumnNames(left_column_count, "__duckdb_delim_col_"));
	auto new_left_bindings = left_cte_ref->GetColumnBindings();
	auto binding_replacements = CreateBindingReplacements(cte_source_bindings, new_left_bindings);

	plan->children[0] = std::move(left_cte_ref);
	ColumnBindingReplacer replacer;
	replacer.replacement_bindings = binding_replacements;
	replacer.stop_operator = plan->children[1];
	replacer.VisitOperator(*plan);

	join.duplicate_eliminated_columns.clear();

	auto dedup_group_index = binder.GenerateTableIndex();
	auto dedup_aggregate_index = binder.GenerateTableIndex();
	vector<unique_ptr<Expression>> dedup_aggrs;
	auto dedup = make_uniq<LogicalAggregate>(dedup_group_index, dedup_aggregate_index, std::move(dedup_aggrs));
	auto dedup_child_index = binder.GenerateTableIndex();
	auto dedup_child = make_uniq<LogicalCTERef>(dedup_child_index, cte_index, left_types,
	                                            GenerateCTEColumnNames(left_column_count, "__duckdb_delim_col_"));
	auto dedup_child_bindings = dedup_child->GetColumnBindings();
	for (idx_t i = 0; i < dedup_column_indices.size(); i++) {
		auto colref = make_uniq<BoundColumnRefExpression>(dedup_names[i], dedup_types[i],
		                                                  dedup_child_bindings[dedup_column_indices[i]]);
		auto new_group_index = ColumnBinding::PushExpression(dedup->groups, std::move(colref));
		for (auto &set : dedup->grouping_sets) {
			set.insert(new_group_index);
		}
	}
	dedup->children.push_back(std::move(dedup_child));

	auto dedup_cte_name = "__duckdb_delim_dedup_" + to_string(dedup_cte_index.index);
	auto dedup_cte =
	    make_uniq<LogicalMaterializedCTE>(dedup_cte_name, dedup_cte_index, dedup_types.size(), std::move(dedup),
	                                      std::move(plan), CTEMaterialize::CTE_MATERIALIZE_DEFAULT);
	auto cte = make_uniq<LogicalMaterializedCTE>(cte_name, cte_index, left_column_count, std::move(cte_source),
	                                             std::move(dedup_cte), CTEMaterialize::CTE_MATERIALIZE_DEFAULT);
	plan = std::move(cte);
}

void DelimJoinCTERewriter::RewriteDelimJoinsToCTEs(unique_ptr<LogicalOperator> &plan) {
	for (auto &child : plan->children) {
		auto old_child_bindings = child->GetColumnBindings();
		RewriteDelimJoinsToCTEs(child);
		RewriteChangedChildBindings(*plan, *child, old_child_bindings);
	}
	if (plan->type == LogicalOperatorType::LOGICAL_DELIM_JOIN) {
		MaterializeDelimJoinAsCTE(plan);
	}
}

void DelimJoinCTERewriter::Rewrite(Binder &binder, unique_ptr<LogicalOperator> &plan) {
	DelimJoinCTERewriter rewriter(binder);
	rewriter.Rewrite(plan);
}

void DelimJoinCTERewriter::Rewrite(unique_ptr<LogicalOperator> &plan) {
	bool filters_pushed;
	do {
		filters_pushed = PushEligibleFiltersIntoDelimJoinInputs(plan);
	} while (filters_pushed);
	RewriteDelimJoinsToCTEs(plan);
	VerifyNoDelim(*plan);
}

} // namespace duckdb
