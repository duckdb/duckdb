//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/subquery/delim_join_cte_rewriter.cpp
//
//
//===----------------------------------------------------------------------===//

#include "duckdb/planner/subquery/delim_join_cte_rewriter.hpp"

#include "duckdb/common/enums/optimizer_type.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/query_profiler.hpp"
#include "duckdb/main/settings.hpp"
#include "duckdb/optimizer/column_binding_replacer.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/planner/logical_operator_visitor.hpp"
#include "duckdb/planner/operator/list.hpp"

#include <algorithm>

namespace duckdb {

static constexpr const char *CTE_DELIMINATOR_PROFILER_KEY = "optimizer.deliminator";

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

static vector<Identifier> GenerateCTEColumnNames(idx_t column_count, const string &prefix) {
	vector<Identifier> result;
	result.reserve(column_count);
	for (idx_t i = 0; i < column_count; i++) {
		result.push_back(Identifier(prefix + to_string(i)));
	}
	return result;
}

static unique_ptr<LogicalOperator> CreateIdentityProjection(Binder &binder, unique_ptr<LogicalOperator> child) {
	child->ResolveOperatorTypes();
	auto bindings = child->GetColumnBindings();
	vector<unique_ptr<Expression>> expressions;
	expressions.reserve(bindings.size());
	for (idx_t i = 0; i < bindings.size(); i++) {
		expressions.push_back(make_uniq<BoundColumnRefExpression>(child->types[i], bindings[i]));
	}
	auto projection = make_uniq<LogicalProjection>(binder.GenerateTableIndex(), std::move(expressions));
	projection->children.push_back(std::move(child));
	projection->ResolveOperatorTypes();
	return std::move(projection);
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

static bool IsColumnEqualityPredicate(Expression &expr) {
	if (!BoundComparisonExpression::IsComparison(expr)) {
		return false;
	}
	switch (expr.GetExpressionType()) {
	case ExpressionType::COMPARE_EQUAL:
	case ExpressionType::COMPARE_NOT_DISTINCT_FROM:
		break;
	default:
		return false;
	}
	auto &comparison = expr.Cast<BoundFunctionExpression>();
	auto &lhs = BoundComparisonExpression::Left(comparison);
	auto &rhs = BoundComparisonExpression::Right(comparison);
	if (lhs.GetExpressionType() != ExpressionType::BOUND_COLUMN_REF ||
	    rhs.GetExpressionType() != ExpressionType::BOUND_COLUMN_REF) {
		return false;
	}
	return lhs.Cast<BoundColumnRefExpression>().Depth() == 0 && rhs.Cast<BoundColumnRefExpression>().Depth() == 0;
}

static bool IsNonSelectiveJoinPredicate(Expression &expr) {
	if (expr.GetExpressionType() == ExpressionType::CONJUNCTION_AND) {
		bool all_children_non_selective = true;
		ExpressionIterator::EnumerateChildren(
		    expr, [&](Expression &child) { all_children_non_selective &= IsNonSelectiveJoinPredicate(child); });
		return all_children_non_selective;
	}
	return IsColumnEqualityPredicate(expr);
}

static bool HasSelection(const LogicalOperator &op) {
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_GET: {
		auto &get = op.Cast<LogicalGet>();
		for (const auto &entry : get.table_filters) {
			auto &expr_filter =
			    ExpressionFilter::GetExpressionFilter(entry.Filter(), "DelimJoinCTERewriter::HasSelection");
			auto &expr = *expr_filter.expr;
			if (expr.GetExpressionClass() != ExpressionClass::BOUND_OPERATOR ||
			    expr.GetExpressionType() != ExpressionType::OPERATOR_IS_NOT_NULL) {
				return true;
			}
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_FILTER: {
		auto &filter = op.Cast<LogicalFilter>();
		for (auto &expr : filter.expressions) {
			if (!IsNonSelectiveJoinPredicate(*expr)) {
				return true;
			}
		}
		break;
	}
	default:
		break;
	}

	for (auto &child : op.children) {
		if (HasSelection(*child)) {
			return true;
		}
	}
	return false;
}

struct JoinWithGeneratedDedupRef {
	JoinWithGeneratedDedupRef(unique_ptr<LogicalOperator> &join_p, idx_t depth_p, bool filter_cross_product_p = false)
	    : join(join_p), depth(depth_p), filter_cross_product(filter_cross_product_p) {
	}

	reference<unique_ptr<LogicalOperator>> join;
	idx_t depth;
	bool filter_cross_product;
};

struct GeneratedDedupRef {
	optional_ptr<LogicalCTERef> cte_ref;
	vector<ColumnBinding> output_bindings;
	vector<unique_ptr<Expression>> output_expressions;
	vector<unique_ptr<Expression>> filters;
	bool has_projection = false;
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

static bool GetBoundColumnRefBinding(Expression &expr, ColumnBinding &binding) {
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

static bool ExpressionReferencesBinding(Expression &expr, const vector<ColumnBinding> &bindings) {
	bool found = false;
	ExpressionIterator::VisitExpression<BoundColumnRefExpression>(expr, [&](const BoundColumnRefExpression &colref) {
		if (colref.Depth() == 0 && FindBindingIndex(bindings, colref.Binding()).IsValid()) {
			found = true;
		}
	});
	return found;
}

static bool CoversAllBindings(const vector<ColumnBinding> &all_bindings,
                              const vector<ColumnBinding> &covered_bindings) {
	if (all_bindings.size() != covered_bindings.size()) {
		return false;
	}
	for (auto &binding : all_bindings) {
		idx_t match_count = 0;
		for (auto &covered_binding : covered_bindings) {
			if (binding == covered_binding) {
				match_count++;
			}
		}
		if (match_count != 1) {
			return false;
		}
	}
	return true;
}

static bool AddExpressionReplacement(vector<ColumnBinding> &bindings, vector<unique_ptr<Expression>> &expressions,
                                     ColumnBinding binding, unique_ptr<Expression> expression) {
	for (idx_t binding_idx = 0; binding_idx < bindings.size(); binding_idx++) {
		if (bindings[binding_idx] != binding) {
			continue;
		}
		return expressions[binding_idx]->Equals(*expression);
	}
	bindings.push_back(binding);
	expressions.push_back(std::move(expression));
	return true;
}

static void ReplaceOperatorBindings(LogicalOperator &op, const vector<ColumnBinding> &bindings,
                                    const vector<unique_ptr<Expression>> &expressions) {
	if (bindings.empty()) {
		return;
	}
	ExpressionBindingReplacer replacer(bindings, expressions);
	replacer.VisitOperator(op);
}

class GeneratedDedupRefEliminator {
public:
	GeneratedDedupRefEliminator(LogicalComparisonJoin &delim_join, TableIndex dedup_cte_index, idx_t dedup_ref_count,
	                            LogicalOperator &rewrite_root);

	idx_t Remove();

private:
	unique_ptr<GeneratedDedupRef> GetGeneratedDedupRef(LogicalOperator &op, bool collect_filters = false,
	                                                   bool allow_projection = false) const;
	bool ExpressionReferencesGeneratedDedupRef(Expression &expr, const GeneratedDedupRef &dedup_ref) const;
	bool AddReplacement(vector<ReplacementBinding> &replacements, ColumnBinding old_binding,
	                    ColumnBinding new_binding) const;
	bool AddReplacement(vector<ReplacementBinding> &replacements, ColumnBinding old_binding, ColumnBinding new_binding,
	                    const LogicalType &new_type) const;
	bool CoversAllDedupColumns(const GeneratedDedupRef &dedup_ref, const vector<ColumnBinding> &bindings) const;
	optional_idx FindGeneratedOutputBinding(Expression &expr, const GeneratedDedupRef &dedup_ref) const;
	bool ExpressionReferencesGeneratedSide(Expression &expr, const GeneratedDedupRef &dedup_ref) const;
	bool FilterIsGeneratedDedupCrossProduct(LogicalOperator &op) const;
	void FindJoinsWithGeneratedDedupRefs(unique_ptr<LogicalOperator> &op, vector<JoinWithGeneratedDedupRef> &joins,
	                                     idx_t depth = 0) const;
	idx_t CountGeneratedDedupRefs(LogicalOperator &op) const;
	bool RemoveInequalityJoinConditions(LogicalOperator &target_op, const vector<JoinCondition> &join_conditions,
	                                    idx_t dedup_idx);
	bool RemoveJoin(unique_ptr<LogicalOperator> &join);
	bool RemoveFilterCrossProduct(unique_ptr<LogicalOperator> &filter_op);

private:
	LogicalComparisonJoin &delim_join;
	TableIndex dedup_cte_index;
	idx_t dedup_ref_count;
	LogicalOperator &rewrite_root;
};

GeneratedDedupRefEliminator::GeneratedDedupRefEliminator(LogicalComparisonJoin &delim_join, TableIndex dedup_cte_index,
                                                         idx_t dedup_ref_count, LogicalOperator &rewrite_root)
    : delim_join(delim_join), dedup_cte_index(dedup_cte_index), dedup_ref_count(dedup_ref_count),
      rewrite_root(rewrite_root) {
}

unique_ptr<GeneratedDedupRef> GeneratedDedupRefEliminator::GetGeneratedDedupRef(LogicalOperator &op,
                                                                                bool collect_filters,
                                                                                bool allow_projection) const {
	if (op.type == LogicalOperatorType::LOGICAL_CTE_REF) {
		auto &cteref = op.Cast<LogicalCTERef>();
		if (cteref.cte_index == dedup_cte_index) {
			auto result = make_uniq<GeneratedDedupRef>();
			result->cte_ref = cteref;
			result->output_bindings = cteref.GetColumnBindings();
			result->output_expressions.reserve(result->output_bindings.size());
			for (idx_t col_idx = 0; col_idx < result->output_bindings.size(); col_idx++) {
				result->output_expressions.push_back(
				    make_uniq<BoundColumnRefExpression>(cteref.chunk_types[col_idx], result->output_bindings[col_idx]));
			}
			return result;
		}
		return nullptr;
	}
	if (op.type == LogicalOperatorType::LOGICAL_FILTER) {
		auto &filter = op.Cast<LogicalFilter>();
		if (filter.HasProjectionMap() || filter.children.size() != 1) {
			return nullptr;
		}
		auto result = GetGeneratedDedupRef(*filter.children[0], collect_filters, allow_projection);
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
	if (allow_projection && op.type == LogicalOperatorType::LOGICAL_PROJECTION) {
		auto &projection = op.Cast<LogicalProjection>();
		if (projection.children.size() != 1) {
			return nullptr;
		}
		auto result = GetGeneratedDedupRef(*projection.children[0], collect_filters, allow_projection);
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
	return nullptr;
}

bool GeneratedDedupRefEliminator::ExpressionReferencesGeneratedDedupRef(Expression &expr,
                                                                        const GeneratedDedupRef &dedup_ref) const {
	bool found = false;
	ExpressionIterator::VisitExpression<BoundColumnRefExpression>(expr, [&](const BoundColumnRefExpression &colref) {
		if (colref.Depth() == 0 && colref.Binding().table_index == dedup_ref.cte_ref->table_index) {
			found = true;
		}
	});
	return found;
}

bool GeneratedDedupRefEliminator::AddReplacement(vector<ReplacementBinding> &replacements, ColumnBinding old_binding,
                                                 ColumnBinding new_binding) const {
	if (old_binding == new_binding) {
		return true;
	}
	for (auto &replacement : replacements) {
		if (replacement.old_binding == old_binding) {
			return replacement.new_binding == new_binding;
		}
	}
	replacements.emplace_back(old_binding, new_binding);
	return true;
}

bool GeneratedDedupRefEliminator::AddReplacement(vector<ReplacementBinding> &replacements, ColumnBinding old_binding,
                                                 ColumnBinding new_binding, const LogicalType &new_type) const {
	if (old_binding == new_binding) {
		return true;
	}
	for (auto &replacement : replacements) {
		if (replacement.old_binding == old_binding) {
			return replacement.new_binding == new_binding;
		}
	}
	replacements.emplace_back(old_binding, new_binding, new_type);
	return true;
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

optional_idx GeneratedDedupRefEliminator::FindGeneratedOutputBinding(Expression &expr,
                                                                     const GeneratedDedupRef &dedup_ref) const {
	optional_idx result;
	bool unsupported = false;
	ExpressionIterator::VisitExpression<BoundColumnRefExpression>(expr, [&](const BoundColumnRefExpression &colref) {
		if (unsupported || colref.Depth() != 0) {
			return;
		}
		auto binding_idx = FindBindingIndex(dedup_ref.output_bindings, colref.Binding());
		if (!binding_idx.IsValid()) {
			return;
		}
		if (result.IsValid() && result.GetIndex() != binding_idx.GetIndex()) {
			unsupported = true;
			return;
		}
		result = binding_idx;
	});
	return unsupported ? optional_idx() : result;
}

bool GeneratedDedupRefEliminator::ExpressionReferencesGeneratedSide(Expression &expr,
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

void GeneratedDedupRefEliminator::FindJoinsWithGeneratedDedupRefs(unique_ptr<LogicalOperator> &op,
                                                                  vector<JoinWithGeneratedDedupRef> &joins,
                                                                  idx_t depth) const {
	if (op->type == LogicalOperatorType::LOGICAL_DELIM_JOIN) {
		if (!op->children.empty()) {
			FindJoinsWithGeneratedDedupRefs(op->children[0], joins, depth + 1);
		}
		return;
	}

	for (auto &child : op->children) {
		FindJoinsWithGeneratedDedupRefs(child, joins, depth + 1);
	}

	if (op->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN &&
	    (GetGeneratedDedupRef(*op->children[0], false, true) || GetGeneratedDedupRef(*op->children[1], false, true))) {
		joins.emplace_back(op, depth);
	} else if (FilterIsGeneratedDedupCrossProduct(*op)) {
		joins.emplace_back(op, depth, true);
	}
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

bool GeneratedDedupRefEliminator::RemoveInequalityJoinConditions(LogicalOperator &target_op,
                                                                 const vector<JoinCondition> &join_conditions,
                                                                 idx_t dedup_idx) {
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

bool GeneratedDedupRefEliminator::RemoveJoin(unique_ptr<LogicalOperator> &join) {
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

	auto dedup_ref = GetGeneratedDedupRef(*join->children[dedup_idx], true, true);
	if (!dedup_ref) {
		return false;
	}

	ColumnBindingReplacer replacer;
	auto &replacement_bindings = replacer.replacement_bindings;
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
		if (!AddReplacement(replacement_bindings, generated_binding, other_binding,
		                    generated_expression.GetReturnType())) {
			return false;
		}

		ColumnBinding base_binding;
		if (GetBoundColumnRefBinding(generated_expression, base_binding) &&
		    base_binding.table_index == dedup_ref->cte_ref->table_index) {
			if (!AddReplacement(replacement_bindings, base_binding, other_binding)) {
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

	vector<unique_ptr<Expression>> filter_expressions;
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
		if (dedup_ref->has_projection ||
		    !RemoveInequalityJoinConditions(*join, comparison_join.conditions, dedup_idx)) {
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

	join = std::move(replacement_op);

	replacer.VisitOperator(rewrite_root);
	ReplaceOperatorBindings(rewrite_root, dedup_ref->output_bindings, generated_output_replacements);
	return true;
}

bool GeneratedDedupRefEliminator::RemoveFilterCrossProduct(unique_ptr<LogicalOperator> &filter_op) {
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

	filter.SplitPredicates();
	vector<bool> consumed(filter.expressions.size(), false);
	vector<JoinCondition> join_conditions;
	ColumnBindingReplacer replacer;
	auto &replacement_bindings = replacer.replacement_bindings;
	bool all_equality_conditions = true;
	vector<ColumnBinding> covered_dedup_bindings;

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
		auto lhs_dedup = lhs_colref.Binding().table_index == dedup_ref->cte_ref->table_index;
		auto rhs_dedup = rhs_colref.Binding().table_index == dedup_ref->cte_ref->table_index;
		if (lhs_dedup == rhs_dedup) {
			continue;
		}

		auto comparison_type = expr.GetExpressionType();
		if (lhs_dedup) {
			if (!AddReplacement(replacement_bindings, lhs_colref.Binding(), rhs_colref.Binding())) {
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
			if (!AddReplacement(replacement_bindings, rhs_colref.Binding(), lhs_colref.Binding())) {
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
	for (idx_t expr_idx = 0; expr_idx < filter.expressions.size(); expr_idx++) {
		if (!consumed[expr_idx] && ExpressionReferencesGeneratedDedupRef(*filter.expressions[expr_idx], *dedup_ref)) {
			return false;
		}
	}

	if (!all_equality_conditions && !RemoveInequalityJoinConditions(*filter_op, join_conditions, dedup_idx)) {
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

idx_t GeneratedDedupRefEliminator::Remove() {
	vector<JoinWithGeneratedDedupRef> joins;
	FindJoinsWithGeneratedDedupRefs(delim_join.children[1], joins);
	if (joins.empty()) {
		return dedup_ref_count;
	}

	std::sort(joins.begin(), joins.end(),
	          [](const JoinWithGeneratedDedupRef &lhs, const JoinWithGeneratedDedupRef &rhs) {
		          return lhs.depth > rhs.depth;
	          });

	if (!joins.empty() && HasSelection(*delim_join.children[0])) {
		joins.erase(joins.begin());
	}

	for (auto &join : joins) {
		if (join.filter_cross_product) {
			RemoveFilterCrossProduct(join.join.get());
		} else {
			RemoveJoin(join.join.get());
		}
	}
	dedup_ref_count = CountGeneratedDedupRefs(*delim_join.children[1]);
	return dedup_ref_count;
}

struct GeneratedDomainRef {
	optional_ptr<LogicalCTERef> cte_ref;
	vector<ColumnBinding> source_bindings;
	vector<ColumnBinding> output_bindings;
	vector<unique_ptr<Expression>> output_expressions;
	vector<unique_ptr<Expression>> filters;
};

class GeneratedDomainJoinEliminator {
public:
	GeneratedDomainJoinEliminator(unique_ptr<LogicalOperator> &rewrite_root,
	                              const vector<TableIndex> &generated_dedup_cte_indexes);

	bool Rewrite();

private:
	void CollectCTEs(LogicalOperator &op);
	optional_ptr<LogicalCTE> FindCTE(TableIndex cte_index) const;
	bool TryRewriteOnce(unique_ptr<LogicalOperator> &op);

	unique_ptr<GeneratedDedupRef> GetGeneratedDedupRef(LogicalOperator &op, bool collect_filters = false,
	                                                   bool allow_projection = false) const;
	unique_ptr<GeneratedDomainRef> GetGeneratedDomainDefinition(LogicalOperator &op) const;
	unique_ptr<GeneratedDomainRef> GetGeneratedDomainRef(LogicalOperator &op, bool collect_filters = false,
	                                                     bool allow_projection = false) const;

	optional_idx FindOutputBinding(Expression &expr, const vector<ColumnBinding> &bindings) const;
	bool ContainsRecursiveCTERef(LogicalOperator &op) const;

	bool AddReplacement(vector<ReplacementBinding> &replacements, ColumnBinding old_binding,
	                    ColumnBinding new_binding) const;
	bool RemoveGeneratedDedupJoin(unique_ptr<LogicalOperator> &join);
	bool RemoveGeneratedDomainJoin(unique_ptr<LogicalOperator> &join);

private:
	unique_ptr<LogicalOperator> &rewrite_root;
	const vector<TableIndex> &generated_dedup_cte_indexes;
	vector<reference<LogicalCTE>> ctes;
};

GeneratedDomainJoinEliminator::GeneratedDomainJoinEliminator(unique_ptr<LogicalOperator> &rewrite_root,
                                                             const vector<TableIndex> &generated_dedup_cte_indexes)
    : rewrite_root(rewrite_root), generated_dedup_cte_indexes(generated_dedup_cte_indexes) {
}

void GeneratedDomainJoinEliminator::CollectCTEs(LogicalOperator &op) {
	if (op.type == LogicalOperatorType::LOGICAL_MATERIALIZED_CTE ||
	    op.type == LogicalOperatorType::LOGICAL_RECURSIVE_CTE) {
		ctes.push_back(op.Cast<LogicalCTE>());
	}
	for (auto &child : op.children) {
		CollectCTEs(*child);
	}
}

optional_ptr<LogicalCTE> GeneratedDomainJoinEliminator::FindCTE(TableIndex cte_index) const {
	for (auto &cte : ctes) {
		if (cte.get().table_index == cte_index) {
			return cte.get();
		}
	}
	return nullptr;
}

unique_ptr<GeneratedDedupRef> GeneratedDomainJoinEliminator::GetGeneratedDedupRef(LogicalOperator &op,
                                                                                  bool collect_filters,
                                                                                  bool allow_projection) const {
	if (op.type == LogicalOperatorType::LOGICAL_CTE_REF) {
		auto &cteref = op.Cast<LogicalCTERef>();
		if (std::find(generated_dedup_cte_indexes.begin(), generated_dedup_cte_indexes.end(), cteref.cte_index) ==
		    generated_dedup_cte_indexes.end()) {
			return nullptr;
		}

		auto result = make_uniq<GeneratedDedupRef>();
		result->cte_ref = cteref;
		result->output_bindings = cteref.GetColumnBindings();
		result->output_expressions.reserve(result->output_bindings.size());
		for (idx_t col_idx = 0; col_idx < result->output_bindings.size(); col_idx++) {
			result->output_expressions.push_back(
			    make_uniq<BoundColumnRefExpression>(cteref.chunk_types[col_idx], result->output_bindings[col_idx]));
		}
		return result;
	}
	if (op.type == LogicalOperatorType::LOGICAL_FILTER) {
		auto &filter = op.Cast<LogicalFilter>();
		if (filter.HasProjectionMap() || filter.children.size() != 1) {
			return nullptr;
		}
		auto result = GetGeneratedDedupRef(*filter.children[0], collect_filters, allow_projection);
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
	if (allow_projection && op.type == LogicalOperatorType::LOGICAL_PROJECTION) {
		auto &projection = op.Cast<LogicalProjection>();
		if (projection.children.size() != 1) {
			return nullptr;
		}
		auto result = GetGeneratedDedupRef(*projection.children[0], collect_filters, allow_projection);
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
	return nullptr;
}

unique_ptr<GeneratedDomainRef> GeneratedDomainJoinEliminator::GetGeneratedDomainDefinition(LogicalOperator &op) const {
	if (op.type == LogicalOperatorType::LOGICAL_CTE_REF) {
		auto &cteref = op.Cast<LogicalCTERef>();
		if (std::find(generated_dedup_cte_indexes.begin(), generated_dedup_cte_indexes.end(), cteref.cte_index) ==
		    generated_dedup_cte_indexes.end()) {
			return nullptr;
		}

		auto result = make_uniq<GeneratedDomainRef>();
		result->source_bindings = cteref.GetColumnBindings();
		result->output_bindings = result->source_bindings;
		result->output_expressions.reserve(result->output_bindings.size());
		for (idx_t col_idx = 0; col_idx < result->output_bindings.size(); col_idx++) {
			result->output_expressions.push_back(
			    make_uniq<BoundColumnRefExpression>(cteref.chunk_types[col_idx], result->output_bindings[col_idx]));
		}
		return result;
	}
	if (op.type == LogicalOperatorType::LOGICAL_FILTER) {
		auto &filter = op.Cast<LogicalFilter>();
		if (filter.HasProjectionMap() || filter.children.size() != 1) {
			return nullptr;
		}
		auto result = GetGeneratedDomainDefinition(*filter.children[0]);
		if (!result) {
			return nullptr;
		}
		for (auto &expr : filter.expressions) {
			auto filter_expr = expr->Copy();
			ReplaceExpressionBindings(filter_expr, result->output_bindings, result->output_expressions);
			result->filters.push_back(std::move(filter_expr));
		}
		return result;
	}
	if (op.type == LogicalOperatorType::LOGICAL_PROJECTION) {
		auto &projection = op.Cast<LogicalProjection>();
		if (projection.children.size() != 1) {
			return nullptr;
		}
		auto result = GetGeneratedDomainDefinition(*projection.children[0]);
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
		return result;
	}
	return nullptr;
}

unique_ptr<GeneratedDomainRef> GeneratedDomainJoinEliminator::GetGeneratedDomainRef(LogicalOperator &op,
                                                                                    bool collect_filters,
                                                                                    bool allow_projection) const {
	if (op.type == LogicalOperatorType::LOGICAL_CTE_REF) {
		auto &cteref = op.Cast<LogicalCTERef>();
		if (std::find(generated_dedup_cte_indexes.begin(), generated_dedup_cte_indexes.end(), cteref.cte_index) !=
		    generated_dedup_cte_indexes.end()) {
			return nullptr;
		}

		auto cte = FindCTE(cteref.cte_index);
		if (!cte || cte->children.empty()) {
			return nullptr;
		}
		if (cte->type == LogicalOperatorType::LOGICAL_RECURSIVE_CTE) {
			return nullptr;
		}
		auto result = GetGeneratedDomainDefinition(*cte->children[0]);
		if (!result || result->output_expressions.size() != cteref.chunk_types.size()) {
			return nullptr;
		}

		result->cte_ref = cteref;
		result->output_bindings = cteref.GetColumnBindings();
		return result;
	}
	if (op.type == LogicalOperatorType::LOGICAL_FILTER) {
		auto &filter = op.Cast<LogicalFilter>();
		if (filter.HasProjectionMap() || filter.children.size() != 1) {
			return nullptr;
		}
		auto result = GetGeneratedDomainRef(*filter.children[0], collect_filters, allow_projection);
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
	if (allow_projection && op.type == LogicalOperatorType::LOGICAL_PROJECTION) {
		auto &projection = op.Cast<LogicalProjection>();
		if (projection.children.size() != 1) {
			return nullptr;
		}
		auto result = GetGeneratedDomainRef(*projection.children[0], collect_filters, allow_projection);
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
		return result;
	}
	return nullptr;
}

optional_idx GeneratedDomainJoinEliminator::FindOutputBinding(Expression &expr,
                                                              const vector<ColumnBinding> &bindings) const {
	optional_idx result;
	bool unsupported = false;
	ExpressionIterator::VisitExpression<BoundColumnRefExpression>(expr, [&](const BoundColumnRefExpression &colref) {
		if (unsupported || colref.Depth() != 0) {
			return;
		}
		auto binding_idx = FindBindingIndex(bindings, colref.Binding());
		if (!binding_idx.IsValid()) {
			return;
		}
		if (result.IsValid() && result.GetIndex() != binding_idx.GetIndex()) {
			unsupported = true;
			return;
		}
		result = binding_idx;
	});
	return unsupported ? optional_idx() : result;
}

bool GeneratedDomainJoinEliminator::ContainsRecursiveCTERef(LogicalOperator &op) const {
	if (op.type == LogicalOperatorType::LOGICAL_CTE_REF) {
		auto &cteref = op.Cast<LogicalCTERef>();
		for (auto &cte : ctes) {
			if (cte.get().table_index == cteref.cte_index) {
				return cte.get().type == LogicalOperatorType::LOGICAL_RECURSIVE_CTE;
			}
		}
	}
	for (auto &child : op.children) {
		if (ContainsRecursiveCTERef(*child)) {
			return true;
		}
	}
	return false;
}

bool GeneratedDomainJoinEliminator::AddReplacement(vector<ReplacementBinding> &replacements, ColumnBinding old_binding,
                                                   ColumnBinding new_binding) const {
	if (old_binding == new_binding) {
		return true;
	}
	for (auto &replacement : replacements) {
		if (replacement.old_binding == old_binding) {
			return replacement.new_binding == new_binding;
		}
	}
	replacements.emplace_back(old_binding, new_binding);
	return true;
}

bool GeneratedDomainJoinEliminator::RemoveGeneratedDedupJoin(unique_ptr<LogicalOperator> &join) {
	auto &comparison_join = join->Cast<LogicalComparisonJoin>();
	if (comparison_join.join_type != JoinType::INNER &&
	    (comparison_join.join_type != JoinType::SEMI || !GetGeneratedDedupRef(*join->children[1], false, true))) {
		return false;
	}

	auto left_is_generated = GetGeneratedDedupRef(*join->children[0], false, true) != nullptr;
	auto right_is_generated = GetGeneratedDedupRef(*join->children[1], false, true) != nullptr;
	if (left_is_generated == right_is_generated) {
		return false;
	}
	const idx_t dedup_idx = left_is_generated ? 0 : 1;
	if (ContainsRecursiveCTERef(*join->children[1 - dedup_idx])) {
		return false;
	}
	auto dedup_ref = GetGeneratedDedupRef(*join->children[dedup_idx], true, true);
	if (!dedup_ref) {
		return false;
	}

	vector<ColumnBinding> covered_dedup_bindings;
	vector<ColumnBinding> base_replacement_bindings;
	vector<unique_ptr<Expression>> base_replacement_expressions;
	vector<unique_ptr<Expression>> join_filter_expressions;
	ColumnBindingReplacer column_replacer;
	auto &replacement_bindings = column_replacer.replacement_bindings;
	for (auto &cond : comparison_join.conditions) {
		if (!cond.IsComparison() || !IsEqualityJoinCondition(cond)) {
			return false;
		}
		auto lhs_generated_idx = FindOutputBinding(cond.GetLHS(), dedup_ref->output_bindings);
		auto rhs_generated_idx = FindOutputBinding(cond.GetRHS(), dedup_ref->output_bindings);
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
		if (!AddReplacement(replacement_bindings, generated_binding, other_binding)) {
			return false;
		}

		ColumnBinding base_binding;
		if (GetBoundColumnRefBinding(generated_expression, base_binding) &&
		    base_binding.table_index == dedup_ref->cte_ref->table_index) {
			if (!AddReplacement(replacement_bindings, base_binding, other_binding)) {
				return false;
			}
			covered_dedup_bindings.emplace_back(base_binding);
			if (!AddExpressionReplacement(base_replacement_bindings, base_replacement_expressions, base_binding,
			                              other_side.Copy())) {
				return false;
			}
		}

		join_filter_expressions.push_back(
		    BoundComparisonExpression::Create(cond.GetComparisonType(), cond.GetLHS().Copy(), cond.GetRHS().Copy()));
	}

	auto cte_bindings = LogicalOperator::GenerateColumnBindings(dedup_ref->cte_ref->table_index,
	                                                            dedup_ref->cte_ref->chunk_types.size());
	if (!CoversAllBindings(cte_bindings, covered_dedup_bindings)) {
		return false;
	}

	vector<unique_ptr<Expression>> generated_output_replacements;
	generated_output_replacements.reserve(dedup_ref->output_expressions.size());
	for (auto &expr : dedup_ref->output_expressions) {
		auto rewritten_expr = expr->Copy();
		ReplaceExpressionBindings(rewritten_expr, base_replacement_bindings, base_replacement_expressions);
		if (ExpressionReferencesBinding(*rewritten_expr, dedup_ref->output_bindings) ||
		    ExpressionReferencesBinding(*rewritten_expr, cte_bindings)) {
			return false;
		}
		generated_output_replacements.push_back(std::move(rewritten_expr));
	}

	vector<unique_ptr<Expression>> filter_expressions;
	for (auto &expr : dedup_ref->filters) {
		ReplaceExpressionBindings(expr, base_replacement_bindings, base_replacement_expressions);
		if (ExpressionReferencesBinding(*expr, dedup_ref->output_bindings) ||
		    ExpressionReferencesBinding(*expr, cte_bindings)) {
			return false;
		}
		filter_expressions.push_back(std::move(expr));
	}
	for (auto &expr : join_filter_expressions) {
		ReplaceExpressionBindings(expr, dedup_ref->output_bindings, generated_output_replacements);
		ReplaceExpressionBindings(expr, base_replacement_bindings, base_replacement_expressions);
		if (ExpressionReferencesBinding(*expr, dedup_ref->output_bindings) ||
		    ExpressionReferencesBinding(*expr, cte_bindings)) {
			return false;
		}
		filter_expressions.push_back(std::move(expr));
	}

	unique_ptr<LogicalOperator> replacement_op = std::move(comparison_join.children[1 - dedup_idx]);
	for (auto &expr : filter_expressions) {
		AddFilterToOperator(replacement_op, std::move(expr));
	}
	join = std::move(replacement_op);

	column_replacer.VisitOperator(*rewrite_root);
	ReplaceOperatorBindings(*rewrite_root, dedup_ref->output_bindings, generated_output_replacements);
	return true;
}

bool GeneratedDomainJoinEliminator::RemoveGeneratedDomainJoin(unique_ptr<LogicalOperator> &join) {
	auto &comparison_join = join->Cast<LogicalComparisonJoin>();
	if (comparison_join.join_type != JoinType::INNER) {
		return false;
	}

	auto left_generated = GetGeneratedDedupRef(*join->children[0], false, true);
	auto right_generated = GetGeneratedDedupRef(*join->children[1], false, true);
	auto left_domain = GetGeneratedDomainRef(*join->children[0], false, true);
	auto right_domain = GetGeneratedDomainRef(*join->children[1], false, true);
	auto left_is_generated = left_generated != nullptr;
	auto right_is_generated = right_generated != nullptr;
	auto left_is_domain = left_domain != nullptr;
	auto right_is_domain = right_domain != nullptr;
	if (left_is_generated == right_is_generated || left_is_domain == right_is_domain) {
		return false;
	}
	if (left_is_generated == left_is_domain) {
		return false;
	}

	const idx_t generated_idx = left_is_generated ? 0 : 1;
	const idx_t domain_idx = 1 - generated_idx;
	auto generated_ref = GetGeneratedDedupRef(*join->children[generated_idx], true, true);
	auto domain_ref = GetGeneratedDomainRef(*join->children[domain_idx], true, true);
	if (!generated_ref || !domain_ref) {
		return false;
	}

	vector<ColumnBinding> source_replacement_bindings;
	vector<unique_ptr<Expression>> source_replacement_expressions;
	vector<ColumnBinding> covered_source_bindings;
	vector<unique_ptr<Expression>> join_filter_expressions;
	for (auto &cond : comparison_join.conditions) {
		if (!cond.IsComparison() || !IsEqualityJoinCondition(cond)) {
			return false;
		}

		auto lhs_generated_idx = FindOutputBinding(cond.GetLHS(), generated_ref->output_bindings);
		auto rhs_generated_idx = FindOutputBinding(cond.GetRHS(), generated_ref->output_bindings);
		auto lhs_domain_idx = FindOutputBinding(cond.GetLHS(), domain_ref->output_bindings);
		auto rhs_domain_idx = FindOutputBinding(cond.GetRHS(), domain_ref->output_bindings);

		const bool lhs_generated = lhs_generated_idx.IsValid();
		const bool rhs_generated = rhs_generated_idx.IsValid();
		const bool lhs_domain = lhs_domain_idx.IsValid();
		const bool rhs_domain = rhs_domain_idx.IsValid();
		if (lhs_generated == rhs_generated || lhs_domain == rhs_domain || lhs_generated == lhs_domain) {
			return false;
		}

		auto domain_output_idx = lhs_domain ? lhs_domain_idx.GetIndex() : rhs_domain_idx.GetIndex();
		auto &domain_expression = *domain_ref->output_expressions[domain_output_idx];
		ColumnBinding source_binding;
		if (GetBoundColumnRefBinding(domain_expression, source_binding) &&
		    FindBindingIndex(domain_ref->source_bindings, source_binding).IsValid()) {
			auto generated_expression = lhs_generated ? cond.GetLHS().Copy() : cond.GetRHS().Copy();
			if (!AddExpressionReplacement(source_replacement_bindings, source_replacement_expressions, source_binding,
			                              std::move(generated_expression))) {
				return false;
			}
			covered_source_bindings.emplace_back(source_binding);
		}

		join_filter_expressions.push_back(
		    BoundComparisonExpression::Create(cond.GetComparisonType(), cond.GetLHS().Copy(), cond.GetRHS().Copy()));
	}
	if (!CoversAllBindings(domain_ref->source_bindings, covered_source_bindings)) {
		return false;
	}

	vector<unique_ptr<Expression>> domain_output_replacements;
	domain_output_replacements.reserve(domain_ref->output_expressions.size());
	for (auto &expr : domain_ref->output_expressions) {
		auto rewritten_expr = expr->Copy();
		ReplaceExpressionBindings(rewritten_expr, source_replacement_bindings, source_replacement_expressions);
		if (ExpressionReferencesBinding(*rewritten_expr, domain_ref->output_bindings) ||
		    ExpressionReferencesBinding(*rewritten_expr, domain_ref->source_bindings)) {
			return false;
		}
		domain_output_replacements.push_back(std::move(rewritten_expr));
	}

	vector<unique_ptr<Expression>> filter_expressions;
	for (auto &expr : domain_ref->filters) {
		ReplaceExpressionBindings(expr, source_replacement_bindings, source_replacement_expressions);
		if (ExpressionReferencesBinding(*expr, domain_ref->output_bindings) ||
		    ExpressionReferencesBinding(*expr, domain_ref->source_bindings)) {
			return false;
		}
		filter_expressions.push_back(std::move(expr));
	}
	for (auto &expr : join_filter_expressions) {
		ReplaceExpressionBindings(expr, domain_ref->output_bindings, domain_output_replacements);
		ReplaceExpressionBindings(expr, source_replacement_bindings, source_replacement_expressions);
		if (ExpressionReferencesBinding(*expr, domain_ref->output_bindings) ||
		    ExpressionReferencesBinding(*expr, domain_ref->source_bindings)) {
			return false;
		}
		filter_expressions.push_back(std::move(expr));
	}

	unique_ptr<LogicalOperator> replacement_op = std::move(comparison_join.children[generated_idx]);
	for (auto &expr : filter_expressions) {
		AddFilterToOperator(replacement_op, std::move(expr));
	}
	join = std::move(replacement_op);

	ReplaceOperatorBindings(*rewrite_root, domain_ref->output_bindings, domain_output_replacements);
	return true;
}

bool GeneratedDomainJoinEliminator::TryRewriteOnce(unique_ptr<LogicalOperator> &op) {
	for (auto &child : op->children) {
		if (TryRewriteOnce(child)) {
			return true;
		}
	}
	if (op->type != LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		return false;
	}
	return RemoveGeneratedDomainJoin(op) || RemoveGeneratedDedupJoin(op);
}

bool GeneratedDomainJoinEliminator::Rewrite() {
	if (generated_dedup_cte_indexes.empty()) {
		return false;
	}

	bool changed = false;
	while (true) {
		ctes.clear();
		CollectCTEs(*rewrite_root);
		if (!TryRewriteOnce(rewrite_root)) {
			break;
		}
		changed = true;
	}
	return changed;
}

static bool SingleJoinRHSIsDeduplicated(LogicalComparisonJoin &join) {
	if (join.join_type != JoinType::SINGLE) {
		return false;
	}

	vector<ColumnBinding> join_bindings;
	for (const auto &cond : join.conditions) {
		if (!IsEqualityJoinCondition(cond)) {
			return false;
		}
		if (!cond.IsComparison() || cond.GetRHS().GetExpressionType() != ExpressionType::BOUND_COLUMN_REF) {
			return false;
		}
		auto &colref = cond.GetRHS().Cast<BoundColumnRefExpression>();
		join_bindings.emplace_back(colref.Binding());
	}

	reference<LogicalOperator> current_op = *join.children[1];
	while (current_op.get().type != LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
		if (current_op.get().children.size() != 1) {
			return false;
		}

		switch (current_op.get().type) {
		case LogicalOperatorType::LOGICAL_PROJECTION:
			if (!FindAndReplaceBindings(join_bindings, current_op.get().expressions,
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

	auto &aggr = current_op.get().Cast<LogicalAggregate>();
	if (!aggr.grouping_functions.empty()) {
		return false;
	}

	for (idx_t group_idx = 0; group_idx < aggr.groups.size(); group_idx++) {
		if (std::find(join_bindings.begin(), join_bindings.end(),
		              ColumnBinding(aggr.group_index, ProjectionIndex(group_idx))) == join_bindings.end()) {
			return false;
		}
	}

	return true;
}

DelimJoinCTERewriter::DelimJoinCTERewriter(Binder &binder) : binder(binder) {
	auto &config = DBConfig::GetConfig(binder.context);
	cte_deliminator_enabled =
	    Settings::Get<EnableOptimizerSetting>(binder.context) &&
	    config.options.disabled_optimizers.find(OptimizerType::DELIMINATOR) == config.options.disabled_optimizers.end();
}

void DelimJoinCTERewriter::MaterializeDelimJoinAsCTE(unique_ptr<LogicalOperator> &plan, LogicalOperator &rewrite_root,
                                                     bool null_rejecting_filter_above) {
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
	if (cte_deliminator_enabled) {
		auto cte_deliminator_timer =
		    QueryProfiler::Get(binder.context).StartTimerInternal(CTE_DELIMINATOR_PROFILER_KEY);
		GeneratedDedupRefEliminator eliminator(join, dedup_cte_index, dedup_ref_count, rewrite_root);
		dedup_ref_count = eliminator.Remove();
		if (SingleJoinRHSIsDeduplicated(join)) {
			join.join_type = null_rejecting_filter_above ? JoinType::INNER : JoinType::LEFT;
		}
	}
	if (dedup_ref_count == 0) {
		join.duplicate_eliminated_columns.clear();
		return;
	}
	generated_dedup_cte_indexes.push_back(dedup_cte_index);

	plan->children[0]->ResolveOperatorTypes();
	auto left_bindings = plan->children[0]->GetColumnBindings();
	auto left_types = plan->children[0]->types;
	auto visible_left_column_count = left_bindings.size();

	vector<idx_t> dedup_column_indices;
	vector<LogicalType> dedup_types;
	vector<Identifier> dedup_names;
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
	auto cte_name = Identifier("__duckdb_delim_" + to_string(cte_index.index));

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

	auto dedup_cte_name = Identifier("__duckdb_delim_dedup_" + to_string(dedup_cte_index.index));
	auto dedup_cte_child = CreateIdentityProjection(binder, std::move(plan));
	auto dedup_cte =
	    make_uniq<LogicalMaterializedCTE>(dedup_cte_name, dedup_cte_index, dedup_types.size(), std::move(dedup),
	                                      std::move(dedup_cte_child), CTEMaterialize::CTE_MATERIALIZE_DEFAULT);
	auto cte_child = CreateIdentityProjection(binder, std::move(dedup_cte));
	auto cte = make_uniq<LogicalMaterializedCTE>(cte_name, cte_index, left_column_count, std::move(cte_source),
	                                             std::move(cte_child), CTEMaterialize::CTE_MATERIALIZE_DEFAULT);
	plan = std::move(cte);
}

void DelimJoinCTERewriter::RewriteDelimJoinsToCTEs(unique_ptr<LogicalOperator> &plan, LogicalOperator &rewrite_root,
                                                   bool null_rejecting_filter_above) {
	for (auto &child : plan->children) {
		auto old_child_bindings = child->GetColumnBindings();
		bool child_null_rejecting_filter_above = false;
		if (plan->type == LogicalOperatorType::LOGICAL_FILTER &&
		    child->type == LogicalOperatorType::LOGICAL_DELIM_JOIN) {
			auto &filter = plan->Cast<LogicalFilter>();
			auto &delim_join = child->Cast<LogicalComparisonJoin>();
			child_null_rejecting_filter_above = FilterNullRejectsDelimJoinRHS(filter, delim_join);
		}
		RewriteDelimJoinsToCTEs(child, rewrite_root, child_null_rejecting_filter_above);
		RewriteChangedChildBindings(*plan, *child, old_child_bindings);
	}
	if (plan->type == LogicalOperatorType::LOGICAL_DELIM_JOIN) {
		MaterializeDelimJoinAsCTE(plan, rewrite_root, null_rejecting_filter_above);
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
	RewriteDelimJoinsToCTEs(plan, *plan);
	if (cte_deliminator_enabled) {
		auto cte_deliminator_timer =
		    QueryProfiler::Get(binder.context).StartTimerInternal(CTE_DELIMINATOR_PROFILER_KEY);
		GeneratedDomainJoinEliminator generated_domain_join_eliminator(plan, generated_dedup_cte_indexes);
		generated_domain_join_eliminator.Rewrite();
	}
	VerifyNoDelim(*plan);
}

} // namespace duckdb
