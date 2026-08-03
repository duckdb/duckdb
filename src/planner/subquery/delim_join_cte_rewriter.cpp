//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/subquery/delim_join_cte_rewriter.cpp
//
//
//===----------------------------------------------------------------------===//

#include "duckdb/planner/subquery/delim_join_cte_rewriter.hpp"

#include "duckdb/main/query_profiler.hpp"
#include "duckdb/optimizer/column_binding_replacer.hpp"
#include "duckdb/planner/subquery/duplicate_eliminated_domain_builder.hpp"
#include "duckdb/planner/subquery/duplicate_eliminated_domain_properties.hpp"
#include "duckdb/planner/column_binding_map.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression_nullability.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/logical_operator_visitor.hpp"
#include "duckdb/planner/operator/list.hpp"

#include <algorithm>

namespace duckdb {

static void InvalidateCardinalityEstimates(LogicalOperator &op) {
	for (auto &child : op.children) {
		InvalidateCardinalityEstimates(*child);
	}
	op.has_estimated_cardinality = false;
}

static constexpr const char *CTE_DELIMINATOR_PROFILER_KEY = "optimizer.deliminator";

static bool CanRewriteDelimJoinAsCTE(const LogicalOperator &op) {
	return !op.HasSideEffects() && !op.HasVolatileExpressions();
}

static void VerifyNoRewriteableDelim(LogicalOperator &op) {
	if (op.type == LogicalOperatorType::LOGICAL_DELIM_JOIN) {
		if (!CanRewriteDelimJoinAsCTE(op)) {
			return;
		}
		throw InternalException("Found DELIM_JOIN after flattening dependent joins");
	}
	if (op.type == LogicalOperatorType::LOGICAL_DELIM_GET) {
		throw InternalException("Found DELIM_GET after flattening dependent joins");
	}
	for (auto &child : op.children) {
		VerifyNoRewriteableDelim(*child);
	}
}

static vector<Identifier> GenerateCTEColumnNames(idx_t column_count, const string &prefix) {
	vector<Identifier> result;
	result.reserve(column_count);
	for (idx_t i = 0; i < column_count; i++) {
		result.push_back(Identifier(prefix + to_string(i)));
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
	if (!GetExpressionColumnBindings(filter, filter_bindings) || filter_bindings.empty()) {
		return false;
	}
	return ChildContainsBindings(*delim_join.children[0], filter_bindings);
}

static bool ExpressionReferencesChild(Expression &expr, LogicalOperator &child) {
	column_binding_set_t expr_bindings;
	if (!GetExpressionColumnBindings(expr, expr_bindings)) {
		return false;
	}
	if (expr_bindings.empty()) {
		return false;
	}
	column_binding_set_t child_bindings;
	for (auto &binding : child.GetColumnBindings()) {
		child_bindings.insert(binding);
	}
	for (auto &binding : expr_bindings) {
		if (child_bindings.find(binding) != child_bindings.end()) {
			return true;
		}
	}
	return false;
}

static bool ExpressionNullPropagatesForChild(Expression &expr, LogicalOperator &child) {
	return ExpressionReferencesChild(expr, child) && expr.PropagatesNullValues();
}

static bool ExpressionNullRejectsDelimJoinRHS(Expression &expr, LogicalComparisonJoin &delim_join) {
	auto &rhs = *delim_join.children[1];
	if (!ExpressionReferencesChild(expr, rhs)) {
		return false;
	}
	if (BoundComparisonExpression::IsComparison(expr)) {
		return expr.PropagatesNullValues();
	}
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_OPERATOR &&
	    expr.GetExpressionType() == ExpressionType::OPERATOR_IS_NOT_NULL) {
		bool null_propagating_child = false;
		ExpressionIterator::EnumerateChildren(expr, [&](Expression &child) {
			null_propagating_child = null_propagating_child || ExpressionNullPropagatesForChild(child, rhs);
		});
		return null_propagating_child;
	}
	return false;
}

static bool FilterNullRejectsDelimJoinRHS(LogicalFilter &filter, LogicalComparisonJoin &delim_join) {
	if (filter.HasProjectionMap() || delim_join.join_type != JoinType::SINGLE) {
		return false;
	}
	for (auto &expr : filter.expressions) {
		if (ExpressionNullRejectsDelimJoinRHS(*expr, delim_join)) {
			return true;
		}
	}
	return false;
}

static bool IsDirectMarkerReference(const Expression &expr, TableIndex mark_index) {
	if (expr.GetExpressionType() != ExpressionType::BOUND_COLUMN_REF) {
		return false;
	}
	auto &column_ref = expr.Cast<BoundColumnRefExpression>();
	return column_ref.Depth() == 0 && column_ref.Binding().table_index == mark_index;
}

static bool FilterRequiresSelectedEvidence(LogicalFilter &filter, LogicalComparisonJoin &delim_join) {
	if (filter.HasProjectionMap() || delim_join.join_type != JoinType::MARK) {
		return false;
	}
	for (auto &expr : filter.expressions) {
		// A direct positive marker reference is null-rejecting and can use the ordinary SEMI rewrite. Negated and
		// otherwise composed marker expressions retain their selected evidence domain.
		if (IsDirectMarkerReference(*expr, delim_join.mark_index)) {
			continue;
		}
		bool found = false;
		ExpressionIterator::VisitExpression<BoundColumnRefExpression>(
		    *expr, [&](const BoundColumnRefExpression &colref) {
			    found |= colref.Depth() == 0 && colref.Binding().table_index == delim_join.mark_index;
		    });
		if (found) {
			return true;
		}
	}
	return false;
}

static bool PushEligibleFilterExpressionsIntoDelimJoinInputs(unique_ptr<LogicalOperator> &plan) {
	auto &filter = plan->Cast<LogicalFilter>();
	if (filter.HasProjectionMap() || filter.children[0]->type != LogicalOperatorType::LOGICAL_DELIM_JOIN) {
		return false;
	}

	auto &delim_join = filter.children[0]->Cast<LogicalComparisonJoin>();
	if (!CanRewriteDelimJoinAsCTE(delim_join)) {
		return false;
	}
	for (auto &expr : filter.expressions) {
		if (expr->IsVolatile() || expr->CanThrow() || expr->HasSubquery()) {
			return false;
		}
	}

	bool changed = false;
	vector<unique_ptr<Expression>> remaining_expressions;
	vector<unique_ptr<Expression>> expressions;
	expressions.reserve(filter.expressions.size());
	for (auto &expr : filter.expressions) {
		expressions.push_back(expr->Copy());
	}
	LogicalFilter::SplitPredicates(expressions);
	for (auto &expr : expressions) {
		if (FilterReferencesDelimInput(delim_join, *expr)) {
			AddFilterToOperator(delim_join.children[0], std::move(expr));
			changed = true;
		} else {
			remaining_expressions.push_back(std::move(expr));
		}
	}
	if (changed) {
		InvalidateCardinalityEstimates(*delim_join.children[0]);
		if (remaining_expressions.empty()) {
			plan = std::move(filter.children[0]);
		} else {
			filter.expressions = std::move(remaining_expressions);
		}
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
	if (changed) {
		plan->has_estimated_cardinality = false;
	}
	return changed;
}

static bool IsEvidenceSide(LogicalOperator &op, idx_t child_idx) {
	if (op.type != LogicalOperatorType::LOGICAL_COMPARISON_JOIN && op.type != LogicalOperatorType::LOGICAL_DELIM_JOIN) {
		return false;
	}
	auto &join = op.Cast<LogicalComparisonJoin>();
	switch (join.join_type) {
	case JoinType::MARK:
	case JoinType::ANTI:
		return child_idx == 1;
	case JoinType::RIGHT_ANTI:
		return child_idx == 0;
	default:
		return false;
	}
}

static bool HasEvidenceSide(JoinType join_type) {
	switch (join_type) {
	case JoinType::MARK:
	case JoinType::ANTI:
	case JoinType::RIGHT_ANTI:
		return true;
	default:
		return false;
	}
}

static bool ContainsSubqueryJoin(LogicalOperator &op) {
	if (op.type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN || op.type == LogicalOperatorType::LOGICAL_DELIM_JOIN) {
		auto &join = op.Cast<LogicalComparisonJoin>();
		switch (join.join_type) {
		case JoinType::MARK:
		case JoinType::SEMI:
		case JoinType::ANTI:
		case JoinType::RIGHT_SEMI:
		case JoinType::RIGHT_ANTI:
		case JoinType::SINGLE:
			return true;
		default:
			break;
		}
	}
	for (auto &child : op.children) {
		if (ContainsSubqueryJoin(*child)) {
			return true;
		}
	}
	return false;
}

struct GeneratedDedupRef {
	optional_ptr<LogicalCTERef> cte_ref;
	vector<ColumnBinding> output_bindings;
	vector<unique_ptr<Expression>> output_expressions;
	vector<unique_ptr<Expression>> filters;
	bool has_projection = false;
};

static bool IsEqualityComparison(ExpressionType comparison_type) {
	switch (comparison_type) {
	case ExpressionType::COMPARE_EQUAL:
	case ExpressionType::COMPARE_NOT_DISTINCT_FROM:
		return true;
	default:
		return false;
	}
}

static bool IsEqualityJoinCondition(const JoinCondition &cond) {
	return cond.IsComparison() && IsEqualityComparison(cond.GetComparisonType());
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

class ExpressionBindingReplacer : public LogicalOperatorVisitor {
public:
	ExpressionBindingReplacer(const vector<ColumnBinding> &bindings, const vector<unique_ptr<Expression>> &expressions)
	    : bindings(bindings), expressions(expressions) {
		D_ASSERT(bindings.size() == expressions.size());
	}

	unique_ptr<Expression> VisitReplace(BoundColumnRefExpression &expr, unique_ptr<Expression> *expr_ptr) override {
		if (expr.Depth() != 0) {
			return nullptr;
		}
		for (idx_t idx = 0; idx < bindings.size(); idx++) {
			if (expr.Binding() == bindings[idx]) {
				return expressions[idx]->Copy();
			}
		}
		return nullptr;
	}

private:
	const vector<ColumnBinding> &bindings;
	const vector<unique_ptr<Expression>> &expressions;
};

static void ReplaceExpressionBindings(unique_ptr<Expression> &expr, const vector<ColumnBinding> &bindings,
                                      const vector<unique_ptr<Expression>> &expressions) {
	if (bindings.empty()) {
		return;
	}
	ExpressionBindingReplacer replacer(bindings, expressions);
	replacer.VisitExpression(&expr);
}

template <class IS_GENERATED>
static unique_ptr<GeneratedDedupRef> ParseGeneratedDedupRef(LogicalOperator &op, const IS_GENERATED &is_generated,
                                                            bool collect_filters, bool allow_projection) {
	if (op.type == LogicalOperatorType::LOGICAL_CTE_REF) {
		auto &cte_ref = op.Cast<LogicalCTERef>();
		if (!is_generated(cte_ref.cte_index)) {
			return nullptr;
		}
		auto result = make_uniq<GeneratedDedupRef>();
		result->cte_ref = cte_ref;
		result->output_bindings = cte_ref.GetColumnBindings();
		result->output_expressions.reserve(result->output_bindings.size());
		for (idx_t col_idx = 0; col_idx < result->output_bindings.size(); col_idx++) {
			result->output_expressions.push_back(
			    make_uniq<BoundColumnRefExpression>(cte_ref.chunk_types[col_idx], result->output_bindings[col_idx]));
		}
		return result;
	}
	if (op.type == LogicalOperatorType::LOGICAL_FILTER) {
		auto &filter = op.Cast<LogicalFilter>();
		if (filter.HasProjectionMap() || filter.children.size() != 1) {
			return nullptr;
		}
		auto result = ParseGeneratedDedupRef(*filter.children[0], is_generated, collect_filters, allow_projection);
		if (!result) {
			return nullptr;
		}
		if (collect_filters) {
			for (auto &expr : filter.expressions) {
				auto filter_expr = expr->Copy();
				ReplaceExpressionBindings(filter_expr, result->output_bindings, result->output_expressions);
				result->filters.push_back(std::move(filter_expr));
			}
		}
		return result;
	}
	if (!allow_projection || op.type != LogicalOperatorType::LOGICAL_PROJECTION) {
		return nullptr;
	}
	auto &projection = op.Cast<LogicalProjection>();
	if (projection.children.size() != 1) {
		return nullptr;
	}
	auto result = ParseGeneratedDedupRef(*projection.children[0], is_generated, collect_filters, allow_projection);
	if (!result) {
		return nullptr;
	}

	auto child_bindings = result->output_bindings;
	auto child_expressions = std::move(result->output_expressions);
	result->output_bindings = projection.GetColumnBindings();
	result->output_expressions.clear();
	result->output_expressions.reserve(projection.expressions.size());
	for (auto &expr : projection.expressions) {
		auto rewritten_expr = expr->Copy();
		ReplaceExpressionBindings(rewritten_expr, child_bindings, child_expressions);
		result->output_expressions.push_back(std::move(rewritten_expr));
	}
	result->has_projection = true;
	return result;
}

static bool GetBoundColumnRefBinding(const Expression &expr, ColumnBinding &binding) {
	if (expr.GetExpressionType() != ExpressionType::BOUND_COLUMN_REF) {
		return false;
	}
	auto &colref = expr.Cast<BoundColumnRefExpression>();
	if (colref.Depth() != 0) {
		return false;
	}
	binding = colref.Binding();
	return true;
}

struct RewrittenOutputLayout {
	vector<ColumnBinding> old_bindings;
	vector<LogicalType> old_types;
	vector<unique_ptr<Expression>> expressions;
	BindingReplacementGraph direct_replacements;
	bool direct = false;
};

static bool ExpressionReferencesOnly(const Expression &expr, const column_binding_set_t &bindings) {
	bool valid = true;
	ExpressionIterator::VisitExpression<BoundColumnRefExpression>(expr, [&](const BoundColumnRefExpression &colref) {
		if (colref.Depth() == 0 && bindings.find(colref.Binding()) == bindings.end()) {
			valid = false;
		}
	});
	return valid;
}

static unique_ptr<RewrittenOutputLayout>
PlanRewrittenOutput(LogicalOperator &old_op, LogicalOperator &retained_output,
                    const vector<ColumnBinding> &replaced_bindings,
                    const vector<unique_ptr<Expression>> &replacement_expressions) {
	if (replaced_bindings.size() != replacement_expressions.size()) {
		return nullptr;
	}
	old_op.ResolveOperatorTypes();
	retained_output.ResolveOperatorTypes();
	auto result = make_uniq<RewrittenOutputLayout>();
	result->old_bindings = old_op.GetColumnBindings();
	result->old_types = old_op.types;
	column_binding_set_t old_binding_set;
	for (auto &binding : result->old_bindings) {
		if (!old_binding_set.insert(binding).second) {
			return nullptr;
		}
	}
	auto retained_bindings = retained_output.GetColumnBindings();
	bool all_replacements_are_columns = true;
	for (idx_t replacement_idx = 0; replacement_idx < replaced_bindings.size(); replacement_idx++) {
		ColumnBinding target;
		auto &replacement = replacement_expressions[replacement_idx];
		if (!GetBoundColumnRefBinding(*replacement, target)) {
			all_replacements_are_columns = false;
			break;
		}
		auto target_idx = FindBindingIndex(retained_bindings, target);
		if (!target_idx.IsValid() || retained_output.types[target_idx.GetIndex()] != replacement->GetReturnType()) {
			all_replacements_are_columns = false;
			break;
		}
		for (idx_t output_idx = 0; output_idx < result->old_bindings.size(); output_idx++) {
			if (result->old_bindings[output_idx] == replaced_bindings[replacement_idx] &&
			    result->old_types[output_idx] != replacement->GetReturnType()) {
				all_replacements_are_columns = false;
				break;
			}
		}
		if (!all_replacements_are_columns) {
			break;
		}
		if (!result->direct_replacements.TryAdd(
		        ReplacementBinding(replaced_bindings[replacement_idx], target, replacement->GetReturnType()))) {
			return nullptr;
		}
	}
	if (all_replacements_are_columns) {
		if (result->old_bindings.size() != retained_bindings.size()) {
			all_replacements_are_columns = false;
		} else {
			for (idx_t output_idx = 0; output_idx < result->old_bindings.size(); output_idx++) {
				if (result->direct_replacements.Resolve(result->old_bindings[output_idx]) !=
				        retained_bindings[output_idx] ||
				    result->old_types[output_idx] != retained_output.types[output_idx]) {
					all_replacements_are_columns = false;
					break;
				}
			}
		}
	}
	if (all_replacements_are_columns) {
		result->direct = true;
		return result;
	}

	column_binding_set_t retained_binding_set(retained_bindings.begin(), retained_bindings.end());
	result->expressions.reserve(result->old_bindings.size());
	for (idx_t output_idx = 0; output_idx < result->old_bindings.size(); output_idx++) {
		auto &old_binding = result->old_bindings[output_idx];
		auto replacement_idx = FindBindingIndex(replaced_bindings, old_binding);
		if (replacement_idx.IsValid()) {
			auto &replacement = replacement_expressions[replacement_idx.GetIndex()];
			if (replacement->GetReturnType() != result->old_types[output_idx] ||
			    !ExpressionReferencesOnly(*replacement, retained_binding_set)) {
				return nullptr;
			}
			result->expressions.push_back(replacement->Copy());
			continue;
		}
		auto retained_idx = FindBindingIndex(retained_bindings, old_binding);
		if (!retained_idx.IsValid() ||
		    retained_output.types[retained_idx.GetIndex()] != result->old_types[output_idx]) {
			return nullptr;
		}
		result->expressions.push_back(make_uniq<BoundColumnRefExpression>(result->old_types[output_idx], old_binding));
	}
	return result;
}

static void InstallRewrittenOutput(Binder &binder, unique_ptr<LogicalOperator> &op,
                                   unique_ptr<LogicalOperator> replacement_op,
                                   unique_ptr<RewrittenOutputLayout> output_layout,
                                   BindingReplacementGraph &replacements) {
	D_ASSERT(output_layout);
	if (output_layout->direct) {
		op = std::move(replacement_op);
		replacements = std::move(output_layout->direct_replacements);
		return;
	}
	auto projection = make_uniq<LogicalProjection>(binder.GenerateTableIndex(), std::move(output_layout->expressions));
	projection->children.push_back(std::move(replacement_op));
	projection->ResolveOperatorTypes();
	auto new_bindings = projection->GetColumnBindings();
	replacements = ColumnBindingRewrite::CreateBindingReplacements(output_layout->old_bindings, new_bindings);
	ColumnBindingRewrite::ValidateOutputLayout(output_layout->old_bindings, output_layout->old_types, new_bindings,
	                                           projection->types, replacements);
	op = std::move(projection);
}

class GeneratedDedupRefEliminator {
public:
	GeneratedDedupRefEliminator(Binder &binder, unique_ptr<LogicalOperator> &delim_join_op, TableIndex dedup_cte_index,
	                            idx_t dedup_ref_count, LogicalOperator &rewrite_root,
	                            bool can_evaluate_additional_groups, bool can_eliminate_equivalent_source_domain);

	idx_t Remove(BindingReplacementGraph &replacements);

private:
	unique_ptr<GeneratedDedupRef> GetGeneratedDedupRef(LogicalOperator &op, bool collect_filters = false,
	                                                   bool allow_projection = false) const;
	bool ExpressionReferencesGeneratedDedupRef(const Expression &expr, const GeneratedDedupRef &dedup_ref) const;
	bool CoversAllDedupColumns(const GeneratedDedupRef &dedup_ref, const vector<ColumnBinding> &bindings) const;
	bool CanReplaceGeneratedOutputsAtBoundary(const GeneratedDedupRef &dedup_ref,
	                                          const vector<unique_ptr<Expression>> &expressions) const;
	optional_idx FindGeneratedOutputBinding(const Expression &expr, const GeneratedDedupRef &dedup_ref) const;
	bool ExpressionReferencesGeneratedSide(const Expression &expr, const GeneratedDedupRef &dedup_ref) const;
	bool FilterIsGeneratedDedupCrossProduct(LogicalOperator &op) const;
	bool RewriteSubtree(unique_ptr<LogicalOperator> &op, BindingReplacementGraph &replacements,
	                    bool under_grouping_operator = false);
	idx_t CountGeneratedDedupRefs(LogicalOperator &op) const;
	bool TryPlanInequalityJoinConditions(LogicalOperator &target_op, const vector<JoinCondition> &join_conditions,
	                                     idx_t dedup_idx, vector<JoinCondition> &rewritten_conditions);
	bool PreserveJoinAsSemi(unique_ptr<LogicalOperator> &join, BindingReplacementGraph &replacements);
	bool PreserveFilterCrossProductAsSemi(unique_ptr<LogicalOperator> &filter_op,
	                                      BindingReplacementGraph &replacements);
	bool RemoveJoin(unique_ptr<LogicalOperator> &join, BindingReplacementGraph &replacements);
	bool RemoveFilterCrossProduct(unique_ptr<LogicalOperator> &filter_op, BindingReplacementGraph &replacements);

private:
	Binder &binder;
	ClientContext &context;
	unique_ptr<LogicalOperator> &delim_join_op;
	LogicalComparisonJoin &delim_join;
	TableIndex dedup_cte_index;
	idx_t dedup_ref_count;
	LogicalOperator &rewrite_root;
	bool can_evaluate_additional_groups;
	bool can_eliminate_equivalent_source_domain;
};

GeneratedDedupRefEliminator::GeneratedDedupRefEliminator(Binder &binder_p, unique_ptr<LogicalOperator> &delim_join_op,
                                                         TableIndex dedup_cte_index, idx_t dedup_ref_count,
                                                         LogicalOperator &rewrite_root,
                                                         bool can_evaluate_additional_groups_p,
                                                         bool can_eliminate_equivalent_source_domain_p)
    : binder(binder_p), context(binder.context), delim_join_op(delim_join_op),
      delim_join(delim_join_op->Cast<LogicalComparisonJoin>()), dedup_cte_index(dedup_cte_index),
      dedup_ref_count(dedup_ref_count), rewrite_root(rewrite_root),
      can_evaluate_additional_groups(can_evaluate_additional_groups_p),
      can_eliminate_equivalent_source_domain(can_eliminate_equivalent_source_domain_p) {
	D_ASSERT(!can_eliminate_equivalent_source_domain || can_evaluate_additional_groups);
}

unique_ptr<GeneratedDedupRef> GeneratedDedupRefEliminator::GetGeneratedDedupRef(LogicalOperator &op,
                                                                                bool collect_filters,
                                                                                bool allow_projection) const {
	return ParseGeneratedDedupRef(
	    op, [&](TableIndex cte_index) { return cte_index == dedup_cte_index; }, collect_filters, allow_projection);
}

bool GeneratedDedupRefEliminator::ExpressionReferencesGeneratedDedupRef(const Expression &expr,
                                                                        const GeneratedDedupRef &dedup_ref) const {
	bool found = false;
	ExpressionIterator::VisitExpression<BoundColumnRefExpression>(expr, [&](const BoundColumnRefExpression &colref) {
		if (colref.Depth() == 0 && colref.Binding().table_index == dedup_ref.cte_ref->table_index) {
			found = true;
		}
	});
	return found;
}

bool GeneratedDedupRefEliminator::CoversAllDedupColumns(const GeneratedDedupRef &dedup_ref,
                                                        const vector<ColumnBinding> &bindings) const {
	auto cte_bindings =
	    LogicalOperator::GenerateColumnBindings(dedup_ref.cte_ref->table_index, dedup_ref.cte_ref->chunk_types.size());
	if (bindings.size() != cte_bindings.size()) {
		return false;
	}
	for (auto &cte_binding : cte_bindings) {
		idx_t match_count = 0;
		for (auto &binding : bindings) {
			if (binding == cte_binding) {
				match_count++;
			}
		}
		if (match_count != 1) {
			return false;
		}
	}
	return true;
}

bool GeneratedDedupRefEliminator::CanReplaceGeneratedOutputsAtBoundary(
    const GeneratedDedupRef &dedup_ref, const vector<unique_ptr<Expression>> &expressions) const {
	D_ASSERT(dedup_ref.output_bindings.size() == expressions.size());
	auto boundary_bindings = delim_join_op->GetColumnBindings();
	for (idx_t expression_idx = 0; expression_idx < expressions.size(); expression_idx++) {
		if (!FindBindingIndex(boundary_bindings, dedup_ref.output_bindings[expression_idx]).IsValid()) {
			continue;
		}
		ColumnBinding replacement;
		if (!GetBoundColumnRefBinding(*expressions[expression_idx], replacement)) {
			return false;
		}
	}
	return true;
}

optional_idx GeneratedDedupRefEliminator::FindGeneratedOutputBinding(const Expression &expr,
                                                                     const GeneratedDedupRef &dedup_ref) const {
	ColumnBinding binding;
	if (!GetBoundColumnRefBinding(expr, binding)) {
		return optional_idx();
	}
	return FindBindingIndex(dedup_ref.output_bindings, binding);
}

bool GeneratedDedupRefEliminator::ExpressionReferencesGeneratedSide(const Expression &expr,
                                                                    const GeneratedDedupRef &dedup_ref) const {
	if (ExpressionReferencesGeneratedDedupRef(expr, dedup_ref)) {
		return true;
	}
	bool found = false;
	ExpressionIterator::VisitExpression<BoundColumnRefExpression>(expr, [&](const BoundColumnRefExpression &colref) {
		if (colref.Depth() != 0) {
			return;
		}
		if (FindBindingIndex(dedup_ref.output_bindings, colref.Binding()).IsValid()) {
			found = true;
		}
	});
	return found;
}

bool GeneratedDedupRefEliminator::FilterIsGeneratedDedupCrossProduct(LogicalOperator &op) const {
	if (op.type != LogicalOperatorType::LOGICAL_FILTER || op.HasProjectionMap()) {
		return false;
	}
	auto &filter = op.Cast<LogicalFilter>();
	if (filter.children.size() != 1 || filter.children[0]->type != LogicalOperatorType::LOGICAL_CROSS_PRODUCT) {
		return false;
	}
	auto &cross_product = *filter.children[0];
	return cross_product.children.size() == 2 &&
	       (GetGeneratedDedupRef(*cross_product.children[0]) || GetGeneratedDedupRef(*cross_product.children[1]));
}

bool GeneratedDedupRefEliminator::RewriteSubtree(unique_ptr<LogicalOperator> &op, BindingReplacementGraph &replacements,
                                                 bool under_grouping_operator) {
	auto old_output_bindings = op->GetColumnBindings();
	if (op->type == LogicalOperatorType::LOGICAL_DELIM_JOIN) {
		if (!op->children.empty()) {
			auto old_child_bindings = op->children[0]->GetColumnBindings();
			BindingReplacementGraph child_replacements;
			if (RewriteSubtree(op->children[0], child_replacements, under_grouping_operator)) {
				ColumnBindingRewrite::ApplyToChild(op, 0, std::move(old_child_bindings), child_replacements);
				replacements = ColumnBindingRewrite::ScopeToOutput(old_output_bindings, op->GetColumnBindings(),
				                                                   child_replacements);
				return true;
			}
		}
		return false;
	}

	bool rewritten = false;
	BindingReplacementGraph accumulated_replacements;
	auto child_under_grouping_operator =
	    under_grouping_operator || op->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY;
	for (idx_t child_idx = 0; child_idx < op->children.size(); child_idx++) {
		auto old_child_bindings = op->children[child_idx]->GetColumnBindings();
		BindingReplacementGraph child_replacements;
		if (RewriteSubtree(op->children[child_idx], child_replacements, child_under_grouping_operator)) {
			ColumnBindingRewrite::ApplyToChild(op, child_idx, std::move(old_child_bindings), child_replacements);
			accumulated_replacements.Merge(child_replacements);
			rewritten = true;
		}
	}

	bool filter_cross_product = false;
	if (op->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN &&
	    (GetGeneratedDedupRef(*op->children[0], false, true) || GetGeneratedDedupRef(*op->children[1], false, true))) {
	} else if (FilterIsGeneratedDedupCrossProduct(*op)) {
		filter_cross_product = true;
	} else {
		if (rewritten) {
			replacements = ColumnBindingRewrite::ScopeToOutput(old_output_bindings, op->GetColumnBindings(),
			                                                   accumulated_replacements);
		}
		return rewritten;
	}

	bool local_rewrite;
	BindingReplacementGraph local_replacements;
	if (!can_evaluate_additional_groups || (under_grouping_operator && !can_eliminate_equivalent_source_domain)) {
		// Unsafe RHSs retain every restriction. A restriction below a grouping operator remains the boundary that
		// prevents global aggregation.
		local_rewrite = filter_cross_product ? PreserveFilterCrossProductAsSemi(op, local_replacements)
		                                     : PreserveJoinAsSemi(op, local_replacements);
	} else {
		local_rewrite = filter_cross_product ? RemoveFilterCrossProduct(op, local_replacements)
		                                     : RemoveJoin(op, local_replacements);
	}
	if (local_rewrite) {
		accumulated_replacements.Merge(local_replacements);
		rewritten = true;
	}
	if (rewritten) {
		replacements =
		    ColumnBindingRewrite::ScopeToOutput(old_output_bindings, op->GetColumnBindings(), accumulated_replacements);
	}
	return rewritten;
}

idx_t GeneratedDedupRefEliminator::CountGeneratedDedupRefs(LogicalOperator &op) const {
	if (op.type == LogicalOperatorType::LOGICAL_DELIM_JOIN) {
		if (!op.children.empty()) {
			return CountGeneratedDedupRefs(*op.children[0]);
		}
		return 0;
	}

	idx_t count = 0;
	if (op.type == LogicalOperatorType::LOGICAL_CTE_REF && op.Cast<LogicalCTERef>().cte_index == dedup_cte_index) {
		count++;
	}
	for (auto &child : op.children) {
		count += CountGeneratedDedupRefs(*child);
	}
	return count;
}

bool GeneratedDedupRefEliminator::TryPlanInequalityJoinConditions(LogicalOperator &target_op,
                                                                  const vector<JoinCondition> &join_conditions,
                                                                  idx_t dedup_idx,
                                                                  vector<JoinCondition> &rewritten_conditions) {
	auto &delim_conditions = delim_join.conditions;
	if (dedup_ref_count != 1 || delim_conditions.size() != join_conditions.size()) {
		return false;
	}
	if (delim_join.join_type != JoinType::ANTI && delim_join.join_type != JoinType::MARK &&
	    delim_join.join_type != JoinType::SEMI && delim_join.join_type != JoinType::SINGLE) {
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
	NotNullExpressionAnalyzer nullability(context, rewrite_root);

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

	rewritten_conditions.reserve(delim_conditions.size());
	for (auto &condition : delim_conditions) {
		rewritten_conditions.push_back(condition.Copy());
	}

	bool found_all = true;
	for (idx_t cond_idx = 0; cond_idx < rewritten_conditions.size(); cond_idx++) {
		auto &delim_condition = rewritten_conditions[cond_idx];
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
				// DISTINCT FROM changes regular inequality semantics when the MARK probe key can be NULL.
				if (delim_join.join_type == JoinType::MARK &&
				    original_join_comparison == ExpressionType::COMPARE_NOTEQUAL) {
					if (!nullability.IsNotNull(*delim_join.children[0], delim_condition.GetLHS())) {
						return false;
					}
				}
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

				rewritten_conditions[cond_idx] = JoinCondition(std::move(left_copy), std::move(right_copy),
				                                               FlipComparisonExpression(join_comparison));
				if (delim_join.join_type != JoinType::MARK &&
				    original_join_comparison != ExpressionType::COMPARE_DISTINCT_FROM &&
				    original_join_comparison != ExpressionType::COMPARE_NOT_DISTINCT_FROM) {
					auto final_comparison = rewritten_conditions[cond_idx].GetComparisonType();
					if (final_comparison == ExpressionType::COMPARE_DISTINCT_FROM) {
						final_comparison = ExpressionType::COMPARE_NOTEQUAL;
					} else if (final_comparison == ExpressionType::COMPARE_NOT_DISTINCT_FROM) {
						final_comparison = ExpressionType::COMPARE_EQUAL;
					}
					rewritten_conditions[cond_idx] =
					    JoinCondition(rewritten_conditions[cond_idx].LeftReference()->Copy(),
					                  rewritten_conditions[cond_idx].RightReference()->Copy(), final_comparison);
				}
				found = true;
				break;
			}
		}
		found_all = found_all && found;
	}

	if (!found_all) {
		return false;
	}
	return true;
}

bool GeneratedDedupRefEliminator::PreserveJoinAsSemi(unique_ptr<LogicalOperator> &join,
                                                     BindingReplacementGraph &replacements) {
	auto &comparison_join = join->Cast<LogicalComparisonJoin>();
	if (comparison_join.join_type != JoinType::INNER && comparison_join.join_type != JoinType::SEMI) {
		return false;
	}
	if (comparison_join.HasProjectionMap()) {
		return false;
	}

	auto left_is_generated = GetGeneratedDedupRef(*join->children[0], false, true) != nullptr;
	auto right_is_generated = GetGeneratedDedupRef(*join->children[1], false, true) != nullptr;
	if (left_is_generated == right_is_generated) {
		return false;
	}
	const idx_t dedup_idx = left_is_generated ? 0 : 1;
	if (comparison_join.join_type == JoinType::SEMI && dedup_idx == 0) {
		return false;
	}

	auto dedup_ref = GetGeneratedDedupRef(*join->children[dedup_idx], false, true);
	if (!dedup_ref) {
		return false;
	}

	BindingReplacementGraph replacement_bindings;
	vector<ColumnBinding> covered_dedup_bindings;
	covered_dedup_bindings.reserve(comparison_join.conditions.size());
	vector<ColumnBinding> base_replacement_bindings;
	vector<unique_ptr<Expression>> base_replacement_expressions;
	vector<JoinCondition> semi_conditions;
	semi_conditions.reserve(comparison_join.conditions.size());

	for (auto &cond : comparison_join.conditions) {
		if (!cond.IsComparison() || !IsEqualityJoinCondition(cond)) {
			return false;
		}

		auto lhs_generated_idx = FindGeneratedOutputBinding(cond.GetLHS(), *dedup_ref);
		auto rhs_generated_idx = FindGeneratedOutputBinding(cond.GetRHS(), *dedup_ref);
		if (lhs_generated_idx.IsValid() == rhs_generated_idx.IsValid()) {
			return false;
		}
		auto generated_idx = lhs_generated_idx.IsValid() ? lhs_generated_idx.GetIndex() : rhs_generated_idx.GetIndex();
		auto &generated_binding = dedup_ref->output_bindings[generated_idx];
		auto &generated_expression = *dedup_ref->output_expressions[generated_idx];
		auto &other_side = lhs_generated_idx.IsValid() ? cond.GetRHS() : cond.GetLHS();

		ColumnBinding other_binding;
		if (!GetBoundColumnRefBinding(other_side, other_binding)) {
			return false;
		}
		if (!replacement_bindings.TryAdd(
		        ReplacementBinding(generated_binding, other_binding, generated_expression.GetReturnType()))) {
			return false;
		}

		ColumnBinding base_binding;
		if (GetBoundColumnRefBinding(generated_expression, base_binding) &&
		    base_binding.table_index == dedup_ref->cte_ref->table_index) {
			if (!replacement_bindings.TryAdd(ReplacementBinding(base_binding, other_binding))) {
				return false;
			}
			covered_dedup_bindings.emplace_back(base_binding);
			base_replacement_bindings.push_back(base_binding);
			base_replacement_expressions.push_back(other_side.Copy());
		}

		auto generated_expr = lhs_generated_idx.IsValid() ? cond.GetLHS().Copy() : cond.GetRHS().Copy();
		auto other_expr = lhs_generated_idx.IsValid() ? cond.GetRHS().Copy() : cond.GetLHS().Copy();
		auto comparison_type =
		    lhs_generated_idx.IsValid() ? FlipComparisonExpression(cond.GetComparisonType()) : cond.GetComparisonType();
		semi_conditions.emplace_back(std::move(other_expr), std::move(generated_expr), comparison_type);
	}
	if (!CoversAllDedupColumns(*dedup_ref, covered_dedup_bindings)) {
		return false;
	}

	vector<unique_ptr<Expression>> generated_output_replacements;
	generated_output_replacements.reserve(dedup_ref->output_expressions.size());
	for (auto &expr : dedup_ref->output_expressions) {
		auto rewritten_expr = expr->Copy();
		ReplaceExpressionBindings(rewritten_expr, base_replacement_bindings, base_replacement_expressions);
		if (ExpressionReferencesGeneratedSide(*rewritten_expr, *dedup_ref)) {
			return false;
		}
		generated_output_replacements.push_back(std::move(rewritten_expr));
	}
	if (!CanReplaceGeneratedOutputsAtBoundary(*dedup_ref, generated_output_replacements)) {
		return false;
	}
	auto output_layout = PlanRewrittenOutput(*join, *comparison_join.children[1 - dedup_idx],
	                                         dedup_ref->output_bindings, generated_output_replacements);
	if (!output_layout) {
		return false;
	}

	if (dedup_idx == 0) {
		std::swap(comparison_join.children[0], comparison_join.children[1]);
	}
	comparison_join.join_type = JoinType::SEMI;
	comparison_join.conditions = std::move(semi_conditions);
	comparison_join.left_projection_map.clear();
	comparison_join.right_projection_map.clear();
	comparison_join.ResolveOperatorTypes();
	auto replacement_op = std::move(join);
	InstallRewrittenOutput(binder, join, std::move(replacement_op), std::move(output_layout), replacements);
	return true;
}

bool GeneratedDedupRefEliminator::PreserveFilterCrossProductAsSemi(unique_ptr<LogicalOperator> &filter_op,
                                                                   BindingReplacementGraph &replacements) {
	auto &filter = filter_op->Cast<LogicalFilter>();
	if (filter.HasProjectionMap() || filter.children.size() != 1 ||
	    filter.children[0]->type != LogicalOperatorType::LOGICAL_CROSS_PRODUCT) {
		return false;
	}
	auto &cross_product = *filter.children[0];
	if (cross_product.children.size() != 2) {
		return false;
	}

	const idx_t dedup_idx = GetGeneratedDedupRef(*cross_product.children[0], false, true) ? 0 : 1;
	auto dedup_ref = GetGeneratedDedupRef(*cross_product.children[dedup_idx], false, true);
	if (!dedup_ref) {
		return false;
	}

	vector<unique_ptr<Expression>> filter_expressions;
	filter_expressions.reserve(filter.expressions.size());
	for (auto &expression : filter.expressions) {
		filter_expressions.push_back(expression->Copy());
	}
	LogicalFilter::SplitPredicates(filter_expressions);
	vector<bool> consumed(filter_expressions.size(), false);
	vector<ColumnBinding> covered_dedup_bindings;
	covered_dedup_bindings.reserve(dedup_ref->output_bindings.size());
	vector<ColumnBinding> base_replacement_bindings;
	vector<unique_ptr<Expression>> base_replacement_expressions;
	vector<JoinCondition> semi_conditions;
	BindingReplacementGraph replacement_bindings;

	for (idx_t expr_idx = 0; expr_idx < filter_expressions.size(); expr_idx++) {
		auto &expr = *filter_expressions[expr_idx];
		if (!BoundComparisonExpression::IsComparison(expr) || !IsEqualityComparison(expr.GetExpressionType())) {
			continue;
		}
		auto &comparison = expr.Cast<BoundFunctionExpression>();
		auto &lhs = BoundComparisonExpression::Left(comparison);
		auto &rhs = BoundComparisonExpression::Right(comparison);

		auto lhs_generated_idx = FindGeneratedOutputBinding(lhs, *dedup_ref);
		auto rhs_generated_idx = FindGeneratedOutputBinding(rhs, *dedup_ref);
		if (lhs_generated_idx.IsValid() == rhs_generated_idx.IsValid()) {
			continue;
		}

		auto generated_idx = lhs_generated_idx.IsValid() ? lhs_generated_idx.GetIndex() : rhs_generated_idx.GetIndex();
		auto &generated_binding = dedup_ref->output_bindings[generated_idx];
		auto &generated_expression = *dedup_ref->output_expressions[generated_idx];
		auto &other_side = lhs_generated_idx.IsValid() ? rhs : lhs;

		ColumnBinding other_binding;
		if (!GetBoundColumnRefBinding(other_side, other_binding)) {
			return false;
		}
		if (!replacement_bindings.TryAdd(
		        ReplacementBinding(generated_binding, other_binding, generated_expression.GetReturnType()))) {
			return false;
		}

		ColumnBinding base_binding;
		if (GetBoundColumnRefBinding(generated_expression, base_binding) &&
		    base_binding.table_index == dedup_ref->cte_ref->table_index) {
			if (!replacement_bindings.TryAdd(ReplacementBinding(base_binding, other_binding))) {
				return false;
			}
			covered_dedup_bindings.emplace_back(base_binding);
			base_replacement_bindings.push_back(base_binding);
			base_replacement_expressions.push_back(other_side.Copy());
		}

		auto generated_expr = lhs_generated_idx.IsValid() ? lhs.Copy() : rhs.Copy();
		auto other_expr = lhs_generated_idx.IsValid() ? rhs.Copy() : lhs.Copy();
		auto comparison_type =
		    lhs_generated_idx.IsValid() ? FlipComparisonExpression(expr.GetExpressionType()) : expr.GetExpressionType();
		semi_conditions.emplace_back(std::move(other_expr), std::move(generated_expr), comparison_type);
		consumed[expr_idx] = true;
	}

	if (semi_conditions.empty() || !CoversAllDedupColumns(*dedup_ref, covered_dedup_bindings)) {
		return false;
	}
	for (idx_t expr_idx = 0; expr_idx < filter_expressions.size(); expr_idx++) {
		if (!consumed[expr_idx] && ExpressionReferencesGeneratedSide(*filter_expressions[expr_idx], *dedup_ref)) {
			return false;
		}
	}

	vector<unique_ptr<Expression>> generated_output_replacements;
	generated_output_replacements.reserve(dedup_ref->output_expressions.size());
	for (auto &expr : dedup_ref->output_expressions) {
		auto rewritten_expr = expr->Copy();
		ReplaceExpressionBindings(rewritten_expr, base_replacement_bindings, base_replacement_expressions);
		if (ExpressionReferencesGeneratedSide(*rewritten_expr, *dedup_ref)) {
			return false;
		}
		generated_output_replacements.push_back(std::move(rewritten_expr));
	}
	if (!CanReplaceGeneratedOutputsAtBoundary(*dedup_ref, generated_output_replacements)) {
		return false;
	}
	auto output_layout = PlanRewrittenOutput(*filter_op, *cross_product.children[1 - dedup_idx],
	                                         dedup_ref->output_bindings, generated_output_replacements);
	if (!output_layout) {
		return false;
	}

	auto semi_join = make_uniq<LogicalComparisonJoin>(JoinType::SEMI);
	semi_join->conditions = std::move(semi_conditions);
	semi_join->children.push_back(std::move(cross_product.children[1 - dedup_idx]));
	semi_join->children.push_back(std::move(cross_product.children[dedup_idx]));
	semi_join->ResolveOperatorTypes();

	unique_ptr<LogicalOperator> replacement_op = std::move(semi_join);
	for (idx_t expr_idx = 0; expr_idx < filter_expressions.size(); expr_idx++) {
		if (consumed[expr_idx]) {
			continue;
		}
		AddFilterToOperator(replacement_op, std::move(filter_expressions[expr_idx]));
	}

	InstallRewrittenOutput(binder, filter_op, std::move(replacement_op), std::move(output_layout), replacements);
	return true;
}

bool GeneratedDedupRefEliminator::RemoveJoin(unique_ptr<LogicalOperator> &join, BindingReplacementGraph &replacements) {
	auto &comparison_join = join->Cast<LogicalComparisonJoin>();
	if (comparison_join.join_type != JoinType::INNER && comparison_join.join_type != JoinType::SEMI) {
		return false;
	}

	auto left_is_generated = GetGeneratedDedupRef(*join->children[0], false, true) != nullptr;
	auto right_is_generated = GetGeneratedDedupRef(*join->children[1], false, true) != nullptr;
	if (left_is_generated == right_is_generated) {
		return false;
	}
	const idx_t dedup_idx = left_is_generated ? 0 : 1;
	if (comparison_join.join_type == JoinType::SEMI && dedup_idx == 0) {
		// A SEMI join emits the duplicate-free left domain. Replacing it with the evidence side can introduce
		// duplicates and change multiplicity even when every output binding can be remapped.
		return false;
	}

	auto dedup_ref = GetGeneratedDedupRef(*join->children[dedup_idx], true, true);
	if (!dedup_ref) {
		return false;
	}

	BindingReplacementGraph replacement_bindings;
	bool all_equality_conditions = true;
	vector<ColumnBinding> covered_dedup_bindings;
	covered_dedup_bindings.reserve(comparison_join.conditions.size());
	vector<ColumnBinding> base_replacement_bindings;
	vector<unique_ptr<Expression>> base_replacement_expressions;
	vector<unique_ptr<Expression>> join_filter_expressions;
	vector<unique_ptr<Expression>> not_null_filter_expressions;

	for (auto &cond : comparison_join.conditions) {
		if (!cond.IsComparison()) {
			return false;
		}
		all_equality_conditions = all_equality_conditions && IsEqualityJoinCondition(cond);

		auto lhs_generated_idx = FindGeneratedOutputBinding(cond.GetLHS(), *dedup_ref);
		auto rhs_generated_idx = FindGeneratedOutputBinding(cond.GetRHS(), *dedup_ref);
		if (lhs_generated_idx.IsValid() == rhs_generated_idx.IsValid()) {
			return false;
		}
		auto generated_idx = lhs_generated_idx.IsValid() ? lhs_generated_idx.GetIndex() : rhs_generated_idx.GetIndex();
		auto &generated_binding = dedup_ref->output_bindings[generated_idx];
		auto &generated_expression = *dedup_ref->output_expressions[generated_idx];
		auto &other_side = lhs_generated_idx.IsValid() ? cond.GetRHS() : cond.GetLHS();

		ColumnBinding other_binding;
		if (!GetBoundColumnRefBinding(other_side, other_binding)) {
			return false;
		}
		if (!replacement_bindings.TryAdd(
		        ReplacementBinding(generated_binding, other_binding, generated_expression.GetReturnType()))) {
			return false;
		}

		ColumnBinding base_binding;
		if (GetBoundColumnRefBinding(generated_expression, base_binding) &&
		    base_binding.table_index == dedup_ref->cte_ref->table_index) {
			if (!replacement_bindings.TryAdd(ReplacementBinding(base_binding, other_binding))) {
				return false;
			}
			covered_dedup_bindings.emplace_back(base_binding);
			base_replacement_bindings.push_back(base_binding);
			base_replacement_expressions.push_back(other_side.Copy());
		}

		join_filter_expressions.push_back(
		    BoundComparisonExpression::Create(cond.GetComparisonType(), cond.GetLHS().Copy(), cond.GetRHS().Copy()));
		if (cond.GetComparisonType() != ExpressionType::COMPARE_NOT_DISTINCT_FROM &&
		    cond.GetComparisonType() != ExpressionType::COMPARE_DISTINCT_FROM) {
			auto is_not_null_expr =
			    make_uniq<BoundOperatorExpression>(ExpressionType::OPERATOR_IS_NOT_NULL, LogicalType::BOOLEAN);
			is_not_null_expr->GetChildrenMutable().push_back(other_side.Copy());
			not_null_filter_expressions.push_back(std::move(is_not_null_expr));
		}
	}
	if (!CoversAllDedupColumns(*dedup_ref, covered_dedup_bindings)) {
		return false;
	}

	vector<unique_ptr<Expression>> generated_output_replacements;
	generated_output_replacements.reserve(dedup_ref->output_expressions.size());
	for (auto &expr : dedup_ref->output_expressions) {
		auto rewritten_expr = expr->Copy();
		ReplaceExpressionBindings(rewritten_expr, base_replacement_bindings, base_replacement_expressions);
		if (ExpressionReferencesGeneratedSide(*rewritten_expr, *dedup_ref)) {
			return false;
		}
		generated_output_replacements.push_back(std::move(rewritten_expr));
	}
	if (!CanReplaceGeneratedOutputsAtBoundary(*dedup_ref, generated_output_replacements)) {
		return false;
	}
	auto output_layout = PlanRewrittenOutput(*join, *comparison_join.children[1 - dedup_idx],
	                                         dedup_ref->output_bindings, generated_output_replacements);
	if (!output_layout) {
		return false;
	}

	vector<unique_ptr<Expression>> filter_expressions;
	vector<JoinCondition> rewritten_delim_conditions;
	if (all_equality_conditions) {
		for (auto &expr : dedup_ref->filters) {
			ReplaceExpressionBindings(expr, base_replacement_bindings, base_replacement_expressions);
			if (ExpressionReferencesGeneratedSide(*expr, *dedup_ref)) {
				return false;
			}
			filter_expressions.push_back(std::move(expr));
		}
		for (auto &expr : join_filter_expressions) {
			ReplaceExpressionBindings(expr, dedup_ref->output_bindings, generated_output_replacements);
			ReplaceExpressionBindings(expr, base_replacement_bindings, base_replacement_expressions);
			if (ExpressionReferencesGeneratedSide(*expr, *dedup_ref)) {
				return false;
			}
			filter_expressions.push_back(std::move(expr));
		}
	} else {
		if (dedup_ref->has_projection || !TryPlanInequalityJoinConditions(*join, comparison_join.conditions, dedup_idx,
		                                                                  rewritten_delim_conditions)) {
			return false;
		}
		filter_expressions = std::move(dedup_ref->filters);
		for (auto &expr : not_null_filter_expressions) {
			filter_expressions.push_back(std::move(expr));
		}
	}

	unique_ptr<LogicalOperator> replacement_op = std::move(comparison_join.children[1 - dedup_idx]);
	if (!filter_expressions.empty()) {
		auto new_filter = make_uniq<LogicalFilter>();
		new_filter->expressions = std::move(filter_expressions);
		new_filter->children.emplace_back(std::move(replacement_op));
		replacement_op = std::move(new_filter);
	}
	if (!all_equality_conditions) {
		delim_join.conditions = std::move(rewritten_delim_conditions);
	}

	InstallRewrittenOutput(binder, join, std::move(replacement_op), std::move(output_layout), replacements);
	return true;
}

bool GeneratedDedupRefEliminator::RemoveFilterCrossProduct(unique_ptr<LogicalOperator> &filter_op,
                                                           BindingReplacementGraph &replacements) {
	auto &filter = filter_op->Cast<LogicalFilter>();
	D_ASSERT(filter.children.size() == 1);
	auto &cross_product = *filter.children[0];
	D_ASSERT(cross_product.type == LogicalOperatorType::LOGICAL_CROSS_PRODUCT);

	const idx_t dedup_idx = GetGeneratedDedupRef(*cross_product.children[0]) ? 0 : 1;
	auto dedup_ref = GetGeneratedDedupRef(*cross_product.children[dedup_idx], true);
	if (!dedup_ref) {
		return false;
	}

	vector<unique_ptr<Expression>> generated_filter_expressions = std::move(dedup_ref->filters);

	vector<unique_ptr<Expression>> filter_expressions;
	filter_expressions.reserve(filter.expressions.size());
	for (auto &expression : filter.expressions) {
		filter_expressions.push_back(expression->Copy());
	}
	LogicalFilter::SplitPredicates(filter_expressions);
	vector<bool> consumed(filter_expressions.size(), false);
	vector<JoinCondition> join_conditions;
	BindingReplacementGraph replacement_bindings;
	bool all_equality_conditions = true;
	vector<ColumnBinding> covered_dedup_bindings;

	for (idx_t expr_idx = 0; expr_idx < filter_expressions.size(); expr_idx++) {
		auto &expr = *filter_expressions[expr_idx];
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
		auto lhs_dedup = lhs_colref.Binding().table_index == dedup_ref->cte_ref->table_index;
		auto rhs_dedup = rhs_colref.Binding().table_index == dedup_ref->cte_ref->table_index;
		if (lhs_dedup == rhs_dedup) {
			continue;
		}

		auto comparison_type = expr.GetExpressionType();
		if (lhs_dedup) {
			if (!replacement_bindings.TryAdd(ReplacementBinding(lhs_colref.Binding(), rhs_colref.Binding()))) {
				return false;
			}
			covered_dedup_bindings.emplace_back(lhs_colref.Binding());
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
			if (!replacement_bindings.TryAdd(ReplacementBinding(rhs_colref.Binding(), lhs_colref.Binding()))) {
				return false;
			}
			covered_dedup_bindings.emplace_back(rhs_colref.Binding());
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

	if (join_conditions.size() != dedup_ref->cte_ref->chunk_types.size() ||
	    !CoversAllDedupColumns(*dedup_ref, covered_dedup_bindings)) {
		return false;
	}
	for (idx_t expr_idx = 0; expr_idx < filter_expressions.size(); expr_idx++) {
		if (!consumed[expr_idx] && ExpressionReferencesGeneratedDedupRef(*filter_expressions[expr_idx], *dedup_ref)) {
			return false;
		}
	}

	vector<JoinCondition> rewritten_delim_conditions;
	if (!all_equality_conditions &&
	    !TryPlanInequalityJoinConditions(*filter_op, join_conditions, dedup_idx, rewritten_delim_conditions)) {
		return false;
	}
	auto &retained_child = *cross_product.children[1 - dedup_idx];
	retained_child.ResolveOperatorTypes();
	auto retained_bindings = retained_child.GetColumnBindings();
	vector<unique_ptr<Expression>> generated_output_replacements;
	generated_output_replacements.reserve(dedup_ref->output_bindings.size());
	for (auto &binding : dedup_ref->output_bindings) {
		auto replacement = replacement_bindings.Resolve(binding);
		auto replacement_idx = FindBindingIndex(retained_bindings, replacement);
		if (replacement == binding || !replacement_idx.IsValid()) {
			return false;
		}
		generated_output_replacements.push_back(
		    make_uniq<BoundColumnRefExpression>(retained_child.types[replacement_idx.GetIndex()], replacement));
	}
	auto output_layout =
	    PlanRewrittenOutput(*filter_op, retained_child, dedup_ref->output_bindings, generated_output_replacements);
	if (!output_layout) {
		return false;
	}

	unique_ptr<LogicalOperator> replacement_op = std::move(cross_product.children[1 - dedup_idx]);
	for (idx_t expr_idx = 0; expr_idx < filter_expressions.size(); expr_idx++) {
		if (!consumed[expr_idx]) {
			generated_filter_expressions.push_back(std::move(filter_expressions[expr_idx]));
		}
	}
	if (!generated_filter_expressions.empty()) {
		auto new_filter = make_uniq<LogicalFilter>();
		new_filter->expressions = std::move(generated_filter_expressions);
		new_filter->children.emplace_back(std::move(replacement_op));
		replacement_op = std::move(new_filter);
	}
	if (!all_equality_conditions) {
		delim_join.conditions = std::move(rewritten_delim_conditions);
	}

	InstallRewrittenOutput(binder, filter_op, std::move(replacement_op), std::move(output_layout), replacements);
	return true;
}

idx_t GeneratedDedupRefEliminator::Remove(BindingReplacementGraph &replacements) {
	auto old_output_bindings = delim_join_op->GetColumnBindings();
	auto old_right_bindings = delim_join.children[1]->GetColumnBindings();
	BindingReplacementGraph right_replacements;
	if (RewriteSubtree(delim_join.children[1], right_replacements)) {
		ColumnBindingRewrite::ApplyToChild(delim_join_op, 1, std::move(old_right_bindings), right_replacements);
		replacements = ColumnBindingRewrite::ScopeToOutput(old_output_bindings, delim_join_op->GetColumnBindings(),
		                                                   right_replacements);
	}
	dedup_ref_count = CountGeneratedDedupRefs(*delim_join_op->children[1]);
	return dedup_ref_count;
}

DelimJoinCTERewriter::DelimJoinCTERewriter(Binder &binder, optional_ptr<DelimJoinCTEOptimization> optimization_p)
    : binder(binder), optimization(optimization_p) {
}

bool DelimJoinCTERewriter::TryInstallOptimizationAlternative(
    unique_ptr<LogicalOperator> &plan, unique_ptr<DelimJoinCTEOptimizationAlternative> alternative,
    const vector<ColumnBinding> &old_output_bindings, const vector<LogicalType> &old_output_types,
    BindingReplacementGraph &output_replacements) {
	D_ASSERT(alternative);
	D_ASSERT(alternative->plan);
	auto alternative_replacements = output_replacements;
	if (!alternative_replacements.TryMerge(alternative->output_replacements)) {
		return false;
	}
	alternative->plan->ResolveOperatorTypes();
	if (!ColumnBindingRewrite::TryValidateOutputLayout(old_output_bindings, old_output_types,
	                                                   alternative->plan->GetColumnBindings(), alternative->plan->types,
	                                                   alternative_replacements)) {
		return false;
	}

	plan = std::move(alternative->plan);
	output_replacements = std::move(alternative_replacements);
	return true;
}

BindingReplacementGraph DelimJoinCTERewriter::RewriteDuplicateEliminatedJoin(unique_ptr<LogicalOperator> &plan,
                                                                             LogicalOperator &rewrite_root,
                                                                             bool null_rejecting_filter_above,
                                                                             bool preserve_evidence_side,
                                                                             bool under_preserved_evidence) {
	plan->ResolveOperatorTypes();
	auto old_output_bindings = plan->GetColumnBindings();
	auto old_output_types = plan->types;
	BindingReplacementGraph output_replacements;
	auto finish_rewrite = [&]() -> BindingReplacementGraph {
		plan->ResolveOperatorTypes();
		auto new_output_bindings = plan->GetColumnBindings();
		ColumnBindingRewrite::ValidateOutputLayout(old_output_bindings, old_output_types, new_output_bindings,
		                                           plan->types, output_replacements);
		return ColumnBindingRewrite::ScopeToOutput(old_output_bindings, new_output_bindings, output_replacements);
	};
	unique_ptr<DelimJoinCTEOptimizationAlternative> optimization_alternative;
	{
		auto &join = plan->Cast<LogicalComparisonJoin>();
		if (join.delim_flipped) {
			throw InternalException("Flatten dependent joins - flipped delim join CTE rewrite not supported");
		}
	}

	plan->type = LogicalOperatorType::LOGICAL_COMPARISON_JOIN;
	plan->Cast<LogicalComparisonJoin>().convert_mark_to_semi = true;
	if (optimization) {
		auto payload_bindings = plan->children[0]->GetColumnBindings();
		plan->children[0]->ResolveOperatorTypes();
		auto payload_types = plan->children[0]->types;
		optimization->PreparePayload(binder, plan->children[0]);
		plan->children[0]->ResolveOperatorTypes();
		ColumnBindingRewrite::ValidateOutputLayout(
		    payload_bindings, payload_types, plan->children[0]->GetColumnBindings(), plan->children[0]->types, {});
		InvalidateCardinalityEstimates(*plan->children[0]);
	}

	auto dedup_cte_index = binder.GenerateTableIndex();
	auto dedup_ref_count = RewriteDelimScanReferences(plan->children[1], dedup_cte_index);
	auto &join = plan->Cast<LogicalComparisonJoin>();
	auto apply_optimization = bool(optimization);
	unique_ptr<DelimJoinCTEOptimizationDecision> optimization_decision;
	auto preserve_selected_evidence =
	    under_preserved_evidence || (preserve_evidence_side && HasEvidenceSide(join.join_type) &&
	                                 DuplicateEliminatedDomainProperties::HasNonJoinSelection(*join.children[0]) &&
	                                 !ContainsSubqueryJoin(*join.children[0]));
	if (!apply_optimization) {
		// Canonical lowering retains the exact observed domain, but represents generated pair-domain joins as SEMI
		// joins wherever possible. This avoids exposing an artificial pair product to join ordering without making
		// the optional decision to evaluate RHS groups outside the observed domain.
		GeneratedDedupRefEliminator eliminator(binder, plan, dedup_cte_index, dedup_ref_count, rewrite_root, false,
		                                       false);
		dedup_ref_count = eliminator.Remove(output_replacements);
		if (DuplicateEliminatedDomainProperties::SingleJoinRHSIsDeduplicated(join, rewrite_root)) {
			join.join_type = null_rejecting_filter_above ? JoinType::INNER : JoinType::LEFT;
		}
	}
	if (apply_optimization) {
		optimization_decision = optimization->Analyze(binder, rewrite_root, join, *join.children[1], dedup_cte_index);
		if (!optimization_decision) {
			throw InternalException("Duplicate-eliminated domain optimization returned no analysis decision");
		}
		auto cte_deliminator_timer =
		    QueryProfiler::Get(binder.context).StartTimerInternal(CTE_DELIMINATOR_PROFILER_KEY);
		GeneratedDedupRefEliminator eliminator(
		    binder, plan, dedup_cte_index, dedup_ref_count, rewrite_root,
		    optimization_decision->CanEvaluateAdditionalGroups() && !preserve_selected_evidence,
		    optimization_decision->CanEliminateEquivalentSourceDomain() && !preserve_selected_evidence);
		dedup_ref_count = eliminator.Remove(output_replacements);
		if (DuplicateEliminatedDomainProperties::SingleJoinRHSIsDeduplicated(join, rewrite_root)) {
			join.join_type = null_rejecting_filter_above ? JoinType::INNER : JoinType::LEFT;
		}
		if (dedup_ref_count > 0) {
			auto result =
			    optimization->TryOptimize(binder, plan, dedup_cte_index, dedup_ref_count, *optimization_decision);
			switch (result.Type()) {
			case DelimJoinCTEOptimizationType::UNCHANGED:
				break;
			case DelimJoinCTEOptimizationType::INLINED:
				join.duplicate_eliminated_columns.clear();
				return finish_rewrite();
			case DelimJoinCTEOptimizationType::ALTERNATIVE:
				optimization_alternative = result.TakeAlternative();
				break;
			default:
				throw InternalException("Unknown duplicate-eliminated domain optimization result");
			}
		}
	}
	if (dedup_ref_count == 0) {
		join.duplicate_eliminated_columns.clear();
		return finish_rewrite();
	}
	if (optimization_alternative &&
	    TryInstallOptimizationAlternative(plan, std::move(optimization_alternative), old_output_bindings,
	                                      old_output_types, output_replacements)) {
		return finish_rewrite();
	}

	plan->children[0]->ResolveOperatorTypes();
	auto left_bindings = plan->children[0]->GetColumnBindings();
	auto left_types = plan->children[0]->types;
	auto visible_left_column_count = left_bindings.size();

	vector<idx_t> dedup_column_indices;
	vector<LogicalType> dedup_types;
	vector<unique_ptr<Expression>> extra_left_expressions;
	dedup_column_indices.reserve(join.duplicate_eliminated_columns.size());
	dedup_types.reserve(join.duplicate_eliminated_columns.size());
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
	}

	if (!extra_left_expressions.empty()) {
		auto old_left_bindings = left_bindings;
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
		vector<ColumnBinding> projected_left_bindings(
		    left_bindings.begin(),
		    left_bindings.begin() + NumericCast<vector<ColumnBinding>::difference_type>(old_left_bindings.size()));
		auto projection_replacements =
		    ColumnBindingRewrite::CreateBindingReplacements(old_left_bindings, projected_left_bindings);
		CorrelatedColumnBindingReplacer projection_replacer;
		projection_replacements.AddTo(projection_replacer);
		projection_replacer.stop_operator = plan->children[1];
		projection_replacer.VisitOperator(*plan);
		output_replacements.Merge(projection_replacements);
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
	auto cte_name = Identifier("__duckdb_delim_" + to_string(cte_index.index));

	auto left_cte_ref_index = binder.GenerateTableIndex();
	auto left_cte_ref = make_uniq<LogicalCTERef>(left_cte_ref_index, cte_index, left_types,
	                                             GenerateCTEColumnNames(left_column_count, "__duckdb_delim_col_"));
	auto new_left_bindings = left_cte_ref->GetColumnBindings();
	auto binding_replacements = ColumnBindingRewrite::CreateBindingReplacements(cte_source_bindings, new_left_bindings);
	output_replacements.Merge(binding_replacements);

	plan->children[0] = std::move(left_cte_ref);
	ColumnBindingReplacer replacer;
	binding_replacements.AddTo(replacer);
	replacer.stop_operator = plan->children[1];
	replacer.VisitOperator(*plan);

	join.duplicate_eliminated_columns.clear();

	auto dedup_child_index = binder.GenerateTableIndex();
	auto dedup_child = make_uniq<LogicalCTERef>(dedup_child_index, cte_index, left_types,
	                                            GenerateCTEColumnNames(left_column_count, "__duckdb_delim_col_"));
	auto dedup =
	    DuplicateEliminatedDomainBuilder::TryBuild(binder, std::move(dedup_child), dedup_column_indices, dedup_types);
	if (!dedup) {
		throw InternalException("Failed to construct duplicate-eliminated domain");
	}

	auto dedup_cte_name = Identifier("__duckdb_delim_dedup_" + to_string(dedup_cte_index.index));
	BindingReplacementGraph dedup_output_replacements;
	auto dedup_cte_child = ColumnBindingRewrite::CreateIdentityProjection(binder.GenerateTableIndex(), std::move(plan),
	                                                                      dedup_output_replacements);
	output_replacements.Merge(dedup_output_replacements);
	auto dedup_cte =
	    make_uniq<LogicalMaterializedCTE>(dedup_cte_name, dedup_cte_index, dedup_types.size(), std::move(dedup),
	                                      std::move(dedup_cte_child), CTEMaterialize::CTE_MATERIALIZE_DEFAULT);
	BindingReplacementGraph cte_output_replacements;
	auto cte_child = ColumnBindingRewrite::CreateIdentityProjection(binder.GenerateTableIndex(), std::move(dedup_cte),
	                                                                cte_output_replacements);
	output_replacements.Merge(cte_output_replacements);
	auto cte = make_uniq<LogicalMaterializedCTE>(cte_name, cte_index, left_column_count, std::move(cte_source),
	                                             std::move(cte_child), CTEMaterialize::CTE_MATERIALIZE_DEFAULT);
	plan = std::move(cte);
	return finish_rewrite();
}

BindingReplacementGraph
DelimJoinCTERewriter::RewriteDelimJoinsToCTEs(unique_ptr<LogicalOperator> &plan, LogicalOperator &rewrite_root,
                                              optional_ptr<bool> plan_changed, bool null_rejecting_filter_above,
                                              bool preserve_evidence_side, bool under_preserved_evidence) {
	auto old_output_bindings = plan->GetColumnBindings();
	BindingReplacementGraph output_replacements;
	if (plan->type == LogicalOperatorType::LOGICAL_DELIM_JOIN && !CanRewriteDelimJoinAsCTE(*plan)) {
		return output_replacements;
	}
	for (idx_t child_idx = 0; child_idx < plan->children.size(); child_idx++) {
		auto &child = plan->children[child_idx];
		auto old_child_bindings = child->GetColumnBindings();
		bool child_null_rejecting_filter_above = false;
		bool child_preserve_evidence_side = false;
		auto child_under_preserved_evidence = under_preserved_evidence;
		if (plan->type == LogicalOperatorType::LOGICAL_FILTER &&
		    child->type == LogicalOperatorType::LOGICAL_DELIM_JOIN) {
			auto &filter = plan->Cast<LogicalFilter>();
			auto &delim_join = child->Cast<LogicalComparisonJoin>();
			child_null_rejecting_filter_above = FilterNullRejectsDelimJoinRHS(filter, delim_join);
			child_preserve_evidence_side = FilterRequiresSelectedEvidence(filter, delim_join);
		}
		if (preserve_evidence_side && IsEvidenceSide(*plan, child_idx)) {
			child_under_preserved_evidence = true;
		}
		bool child_changed = false;
		auto child_replacements =
		    RewriteDelimJoinsToCTEs(child, rewrite_root, child_changed, child_null_rejecting_filter_above,
		                            child_preserve_evidence_side, child_under_preserved_evidence);
		ColumnBindingRewrite::ApplyToChild(plan, child_idx, std::move(old_child_bindings), child_replacements);
		if (child_changed) {
			plan->has_estimated_cardinality = false;
			if (plan_changed) {
				*plan_changed = true;
			}
		}
		output_replacements.Merge(child_replacements);
		output_replacements =
		    ColumnBindingRewrite::ScopeToOutput(old_output_bindings, plan->GetColumnBindings(), output_replacements);
	}
	if (plan->type == LogicalOperatorType::LOGICAL_DELIM_JOIN) {
		auto rewrite_replacements = RewriteDuplicateEliminatedJoin(plan, rewrite_root, null_rejecting_filter_above,
		                                                           preserve_evidence_side, under_preserved_evidence);
		InvalidateCardinalityEstimates(*plan);
		if (plan_changed) {
			*plan_changed = true;
		}
		output_replacements.Merge(rewrite_replacements);
	}
	return ColumnBindingRewrite::ScopeToOutput(old_output_bindings, plan->GetColumnBindings(), output_replacements);
}

static void NormalizeInputs(unique_ptr<LogicalOperator> &plan) {
	bool filters_pushed;
	do {
		filters_pushed = PushEligibleFiltersIntoDelimJoinInputs(plan);
	} while (filters_pushed);
}

void DelimJoinCTERewriter::Rewrite(Binder &binder, unique_ptr<LogicalOperator> &plan,
                                   optional_ptr<DelimJoinCTEOptimization> optimization) {
	NormalizeInputs(plan);
	DelimJoinCTERewriter rewriter(binder, optimization);
	rewriter.RewriteInternal(plan);
}

void DelimJoinCTERewriter::RewriteInternal(unique_ptr<LogicalOperator> &plan) {
	RewriteDelimJoinsToCTEs(plan, *plan);
	VerifyNoRewriteableDelim(*plan);
}

} // namespace duckdb
