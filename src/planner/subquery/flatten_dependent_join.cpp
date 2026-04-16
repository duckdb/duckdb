#include "duckdb/planner/subquery/flatten_dependent_join.hpp"

#include "duckdb/common/operator/add.hpp"
#include "duckdb/common/exception/parser_exception.hpp"
#include "duckdb/function/aggregate/distributive_functions.hpp"
#include "duckdb/function/aggregate/distributive_function_utils.hpp"
#include "duckdb/function/window/rows_functions.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/list.hpp"
#include "duckdb/planner/operator/list.hpp"
#include "duckdb/planner/subquery/has_correlated_expressions.hpp"
#include "duckdb/planner/subquery/rewrite_correlated_expressions.hpp"
#include "duckdb/planner/subquery/rewrite_cte_scan.hpp"
#include "duckdb/planner/operator/logical_dependent_join.hpp"

namespace duckdb {

FlattenDependentJoins::FlattenDependentJoins(Binder &binder, const CorrelatedColumns &correlated, bool perform_delim,
                                             bool any_join, optional_ptr<FlattenDependentJoins> parent)
    : binder(binder), correlated_columns(correlated), perform_delim(perform_delim), any_join(any_join), parent(parent) {
	if (parent) {
		equivalent_bindings = parent->equivalent_bindings;
	}
	for (idx_t i = 0; i < correlated_columns.size(); i++) {
		auto &col = correlated_columns[i];
		correlated_map[col.binding] = i;
		if (equivalent_bindings.find(col.binding) == equivalent_bindings.end()) {
			equivalent_bindings[col.binding] = col.binding;
		}
		auto canonical_binding = GetCanonicalBinding(col.binding);
		if (canonical_correlated_map.find(canonical_binding) == canonical_correlated_map.end()) {
			canonical_correlated_map[canonical_binding] = i;
		}
		delim_types.push_back(col.type);
	}
}

void FlattenDependentJoins::CreateDelimJoinConditions(LogicalComparisonJoin &delim_join,
                                                      const CorrelatedColumns &correlated_columns,
                                                      vector<ColumnBinding> bindings, const CorrelatedLayout &layout,
                                                      bool perform_delim) {
	// Determine the range of columns to process
	idx_t start = 0;
	idx_t end = perform_delim ? correlated_columns.size() : 1;

	// Special case: if not doing a full delim join, use the specific delim index if it's valid
	if (!perform_delim && correlated_columns.GetDelimIndex() < correlated_columns.size()) {
		start = correlated_columns.GetDelimIndex();
		end = start + 1;
	}

	for (idx_t i = start; i < end; i++) {
		auto &col = correlated_columns[i];
		auto binding_idx = layout.GetOffset(i);
		if (binding_idx >= bindings.size()) {
			throw InternalException("Delim join - binding index out of range");
		}
		JoinCondition cond(make_uniq<BoundColumnRefExpression>(col.name, col.type, col.binding),
		                   make_uniq<BoundColumnRefExpression>(col.name, col.type, bindings[binding_idx]),
		                   ExpressionType::COMPARE_NOT_DISTINCT_FROM);
		delim_join.conditions.push_back(std::move(cond));
	}
}

static vector<ColumnBinding> GetDependentJoinPlanColumns(LogicalOperator &op) {
	if (op.type == LogicalOperatorType::LOGICAL_MATERIALIZED_CTE) {
		return op.children[1]->GetColumnBindings();
	}
	return op.GetColumnBindings();
}

static void PopulateDuplicateEliminatedColumns(LogicalDependentJoin &op) {
	op.duplicate_eliminated_columns.clear();
	op.mark_types.clear();
	for (idx_t i = 0; i < op.correlated_columns.size(); i++) {
		auto &col = op.correlated_columns[i];
		op.duplicate_eliminated_columns.push_back(make_uniq<BoundColumnRefExpression>(col.type, col.binding));
		op.mark_types.push_back(col.type);
	}
}

unique_ptr<LogicalOperator> FlattenDependentJoins::DecorrelateIndependent(Binder &binder,
                                                                          unique_ptr<LogicalOperator> plan) {
	CorrelatedColumns correlated;
	FlattenDependentJoins flatten(binder, correlated);
	return flatten.Decorrelate(std::move(plan)).plan;
}

FlattenDependentJoins::PushDownResult
FlattenDependentJoins::Decorrelate(unique_ptr<LogicalOperator> plan, PushDownContext context, CorrelatedLayout layout) {
	switch (plan->type) {
	case LogicalOperatorType::LOGICAL_DEPENDENT_JOIN: {
		return DecorrelateDependentJoin(std::move(plan), context, std::move(layout));
	}
	default: {
		for (auto &child : plan->children) {
			auto child_result = Decorrelate(std::move(child), context.WithFreshTraversal(), layout);
			child = std::move(child_result.plan);
			layout = std::move(child_result.layout);
		}
	}
	}

	return PushDownResult(std::move(plan), std::move(layout));
}

FlattenDependentJoins::PushDownResult FlattenDependentJoins::DecorrelateDependentJoin(unique_ptr<LogicalOperator> plan,
                                                                                      PushDownContext context,
                                                                                      CorrelatedLayout layout) {
	auto &delim_join = plan;
	auto &op = plan->Cast<LogicalDependentJoin>();
	layout = PrepareDependentJoinLeft(op, context, std::move(layout));

	auto flatten_context = PushDownContext(op.propagate_null_values, 0);
	FlattenDependentJoins flatten(binder, op.correlated_columns, op.perform_delim, op.any_join, this);

	// first we check which logical operators have correlated expressions in the first place
	flatten.DetectCorrelatedExpressions(*delim_join->children[1], op.is_lateral_join, flatten_context.lateral_depth);

	if (delim_join->children[1]->type == LogicalOperatorType::LOGICAL_MATERIALIZED_CTE) {
		auto &cte_ref = delim_join->children[1]->Cast<LogicalMaterializedCTE>();
		// check if the left side of the CTE has correlated expressions
		auto entry = flatten.has_correlated_expressions.find(*cte_ref.children[0]);
		if (entry != flatten.has_correlated_expressions.end() && !entry->second) {
			// the left side of the CTE has no correlated expressions, we can push the DEPENDENT_JOIN down
			auto cte = std::move(delim_join->children[1]);
			delim_join->children[1] = std::move(cte->children[1]);
			auto decorrelated =
			    Decorrelate(std::move(delim_join), context.WithLateralDepth(flatten_context.lateral_depth), layout);
			cte->children[1] = std::move(decorrelated.plan);
			return PushDownResult(std::move(cte), std::move(decorrelated.layout));
		}
	}

	// now we push the dependent join down
	auto flatten_result = flatten.PushDownDependentJoin(std::move(delim_join->children[1]), flatten_context);
	delim_join->children[1] = std::move(flatten_result.plan);
	if (!parent) {
		layout = flatten_result.layout;
		layout.ShiftOffsets(delim_join->children[0]->GetColumnBindings().size());
	}
	return FinalizeDependentJoin(std::move(plan), std::move(layout), flatten_result.layout,
	                             flatten_context.lateral_depth);
}

FlattenDependentJoins::CorrelatedLayout FlattenDependentJoins::PrepareDependentJoinLeft(LogicalDependentJoin &op,
                                                                                        PushDownContext context,
                                                                                        CorrelatedLayout layout) {
	// If we have a parent, we unnest the left side of the DEPENDENT JOIN in the parent's context.
	if (parent) {
		// only push the dependent join to the left side, if there is correlation.
		auto entry = has_correlated_expressions.find(op);
		D_ASSERT(entry != has_correlated_expressions.end());

		if (entry->second) {
			auto left_result = PushDownDependentJoin(std::move(op.children[0]), context, layout);
			op.children[0] = std::move(left_result.plan);
			layout = std::move(left_result.layout);
		} else {
			// There might be unrelated correlation, so we have to traverse the tree
			op.children[0] = DecorrelateIndependent(binder, std::move(op.children[0]));
		}

		// we are now done with the left side, mark it as uncorrelated
		entry->second = false;

		RewriteCorrelatedOperator(op, layout, 0);
		RewriteCorrelatedOperator(op, layout, 0, true);
	} else {
		auto left_result = Decorrelate(std::move(op.children[0]), context.WithFreshTraversal(), std::move(layout));
		op.children[0] = std::move(left_result.plan);
		layout = std::move(left_result.layout);
	}

	if (!op.perform_delim) {
		// if we are not performing a delim join, we push a row_number() OVER() window operator on the LHS
		// and perform all duplicate elimination on that row number instead
		const auto &op_col = op.correlated_columns[op.correlated_columns.GetDelimIndex()];
		auto window = make_uniq<LogicalWindow>(op_col.binding.table_index);
		auto row_number_func = make_uniq<WindowFunction>(RowNumberFun::GetFunction());
		auto row_number =
		    make_uniq<BoundWindowExpression>(LogicalType::BIGINT, nullptr, std::move(row_number_func), nullptr);
		row_number->start = WindowBoundary::UNBOUNDED_PRECEDING;
		row_number->end = WindowBoundary::CURRENT_ROW_ROWS;
		row_number->SetAlias("delim_index");
		window->expressions.push_back(std::move(row_number));
		window->AddChild(std::move(op.children[0]));
		op.children[0] = std::move(window);
	}
	return layout;
}

void FlattenDependentJoins::AddAnyJoinConditions(LogicalDependentJoin &op,
                                                 const vector<ColumnBinding> &plan_columns) const {
	// add the actual condition based on the ANY/ALL predicate
	for (idx_t child_idx = 0; child_idx < op.expression_children.size(); child_idx++) {
		auto left_expr = std::move(op.expression_children[child_idx]);
		auto &child_type = op.child_types[child_idx];
		auto &compare_type = op.child_targets[child_idx];
		auto right_expr = BoundCastExpression::AddDefaultCastToType(
		    make_uniq<BoundColumnRefExpression>(child_type, plan_columns[child_idx]), op.child_targets[child_idx]);
		JoinCondition compare_cond(std::move(left_expr), std::move(right_expr), op.comparison_type);

		// push collations
		ExpressionBinder::PushCollation(binder.context, compare_cond.LeftReference(), compare_type);
		ExpressionBinder::PushCollation(binder.context, compare_cond.RightReference(), compare_type);
		op.conditions.push_back(std::move(compare_cond));
	}
}

void FlattenDependentJoins::AddComparisonJoinConditions(LogicalComparisonJoin &join,
                                                        const CorrelatedLayout &left_layout,
                                                        const CorrelatedLayout &right_layout) const {
	for (idx_t i = 0; i < correlated_columns.size(); i++) {
		JoinCondition cond(make_uniq<BoundColumnRefExpression>(correlated_columns[i].type, left_layout.GetBinding(i)),
		                   make_uniq<BoundColumnRefExpression>(correlated_columns[i].type, right_layout.GetBinding(i)),
		                   ExpressionType::COMPARE_NOT_DISTINCT_FROM);
		join.conditions.push_back(std::move(cond));
	}
}

void FlattenDependentJoins::AddCTERefJoinConditions(LogicalComparisonJoin &join, const LogicalCTERef &cteref,
                                                    const CorrelatedLayout &layout) const {
	if (cteref.correlated_columns == 0) {
		return;
	}
	auto rec_cte = binder.recursive_ctes.find(cteref.cte_index);
	if (rec_cte == binder.recursive_ctes.end()) {
		return;
	}
	auto &cte_corr_cols = rec_cte->second->Cast<LogicalCTE>().correlated_columns;
	D_ASSERT(cteref.correlated_columns <= cte_corr_cols.size());
	auto cte_ref_offset = cteref.chunk_types.size() - cteref.correlated_columns;
	auto cte_corr_start = cte_corr_cols.size() - cteref.correlated_columns;
	for (idx_t i = 0; i < cteref.correlated_columns; i++) {
		auto canonical_binding = GetCanonicalBinding(cte_corr_cols[cte_corr_start + i].binding);
		auto entry = canonical_correlated_map.find(canonical_binding);
		if (entry == canonical_correlated_map.end()) {
			continue;
		}
		auto j = entry->second;
		JoinCondition cond(
		    make_uniq<BoundColumnRefExpression>(correlated_columns[j].type,
		                                        ColumnBinding(cteref.table_index, ProjectionIndex(cte_ref_offset + i))),
		    make_uniq<BoundColumnRefExpression>(correlated_columns[j].type, layout.GetBinding(j)),
		    ExpressionType::COMPARE_NOT_DISTINCT_FROM);
		join.conditions.push_back(std::move(cond));
	}
}

void FlattenDependentJoins::AddCorrelatedJoinConditions(LogicalJoin &join, const CorrelatedLayout &left_layout,
                                                        const CorrelatedLayout &right_layout) const {
	if (join.type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN ||
	    join.type == LogicalOperatorType::LOGICAL_ASOF_JOIN) {
		AddComparisonJoinConditions(join.Cast<LogicalComparisonJoin>(), left_layout, right_layout);
		return;
	}
	auto &logical_any_join = join.Cast<LogicalAnyJoin>();
	for (idx_t i = 0; i < correlated_columns.size(); i++) {
		auto left = make_uniq<BoundColumnRefExpression>(correlated_columns[i].type, left_layout.GetBinding(i));
		auto right = make_uniq<BoundColumnRefExpression>(correlated_columns[i].type, right_layout.GetBinding(i));
		auto comparison = make_uniq<BoundComparisonExpression>(ExpressionType::COMPARE_NOT_DISTINCT_FROM,
		                                                       std::move(left), std::move(right));
		auto conjunction = make_uniq<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_AND, std::move(comparison),
		                                                         std::move(logical_any_join.condition));
		logical_any_join.condition = std::move(conjunction);
	}
}

FlattenDependentJoins::PushDownResult FlattenDependentJoins::FinalizeDependentJoin(unique_ptr<LogicalOperator> plan,
                                                                                   CorrelatedLayout layout,
                                                                                   const CorrelatedLayout &right_layout,
                                                                                   idx_t lateral_depth) {
	auto &op = plan->Cast<LogicalDependentJoin>();
	RewriteCorrelatedOperator(*plan, layout, lateral_depth);
	PopulateDuplicateEliminatedColumns(op);

	// We are done using the operator as a DEPENDENT JOIN, it is now fully decorrelated,
	// and we change the type to a DELIM JOIN.
	plan->type = LogicalOperatorType::LOGICAL_DELIM_JOIN;

	auto plan_columns = GetDependentJoinPlanColumns(*plan->children[1]);
	CreateDelimJoinConditions(op, op.correlated_columns, plan_columns, right_layout, op.perform_delim);

	if (op.is_lateral_join && op.subquery_type == SubqueryType::INVALID) {
		// check if there are any arbitrary expressions left
		if (!op.arbitrary_expressions.empty()) {
			// we can only evaluate scalar arbitrary expressions for inner joins
			if (op.join_type != JoinType::INNER) {
				throw BinderException("Join condition for non-inner LATERAL JOIN must be a comparison between the left "
				                      "and right side");
			}
			auto filter = make_uniq<LogicalFilter>();
			filter->expressions = std::move(op.arbitrary_expressions);
			filter->AddChild(std::move(plan));
			return PushDownResult(std::move(filter), std::move(layout));
		}
		return PushDownResult(std::move(plan), std::move(layout));
	}

	if (op.subquery_type == SubqueryType::ANY) {
		AddAnyJoinConditions(op, plan_columns);
	}
	return PushDownResult(std::move(plan), std::move(layout));
}

FlattenDependentJoins::PushDownResult
FlattenDependentJoins::PushDownSingleCorrelatedChild(unique_ptr<LogicalOperator> plan, PushDownContext context,
                                                     CorrelatedLayout layout, bool correlated_left) {
	auto correlated_idx = correlated_left ? 0 : 1;
	auto independent_idx = correlated_left ? 1 : 0;
	layout = PushDownChild(plan->children[correlated_idx], context, std::move(layout));
	plan->children[independent_idx] = DecorrelateIndependent(binder, std::move(plan->children[independent_idx]));
	if (!correlated_left) {
		layout.ShiftOffsets(plan->children[0]->GetColumnBindings().size());
	}
	return PushDownResult(std::move(plan), std::move(layout));
}

bool FlattenDependentJoins::DetectCorrelatedExpressions(LogicalOperator &op, bool lateral, idx_t lateral_depth,
                                                        bool parent_is_dependent_join) {
	if (op.type == LogicalOperatorType::LOGICAL_CTE_REF) {
		auto &cteref = op.Cast<LogicalCTERef>();
		bool correlated = cteref.correlated_columns > 0 && cteref.correlated_columns == correlated_columns.size();
		has_correlated_expressions[op] = correlated;
		return correlated;
	}

	bool is_lateral_join = false;

	// check if this entry has correlated expressions
	if (op.type == LogicalOperatorType::LOGICAL_DEPENDENT_JOIN) {
		is_lateral_join = true;
	}
	HasCorrelatedExpressions visitor(correlated_columns, lateral, lateral_depth);
	visitor.VisitOperator(op);
	bool has_correlation = visitor.has_correlated_expressions;
	int child_idx = 0;
	// now visit the children of this entry and check if they have correlated expressions
	for (auto &child : op.children) {
		auto new_lateral_depth = lateral_depth;
		if (is_lateral_join && child_idx == 1) {
			new_lateral_depth = lateral_depth + 1;
		}
		// we OR the property with its children such that has_correlation is true if either
		// (1) this node has a correlated expression or
		// (2) one of its children has a correlated expression
		bool condition = (parent_is_dependent_join || is_lateral_join) && child_idx == 0;
		if (DetectCorrelatedExpressions(*child, lateral, new_lateral_depth, condition)) {
			has_correlation = true;
		}

		if (op.type == LogicalOperatorType::LOGICAL_MATERIALIZED_CTE && child_idx == 0) {
			auto &setop = op.Cast<LogicalCTE>();
			binder.recursive_ctes[setop.table_index] = &setop;
		}

		child_idx++;
	}

	// set the entry in the map
	has_correlated_expressions[op] = has_correlation;

	if (op.type == LogicalOperatorType::LOGICAL_RECURSIVE_CTE) {
		auto &setop = op.Cast<LogicalCTE>();
		binder.recursive_ctes[setop.table_index] = &setop;
	}

	return has_correlation;
}

FlattenDependentJoins::PushDownResult FlattenDependentJoins::PushDownDependentJoin(unique_ptr<LogicalOperator> plan,
                                                                                   PushDownContext context,
                                                                                   CorrelatedLayout layout) {
	auto result = PushDownDependentJoinInternal(std::move(plan), context, std::move(layout));
	if (!replacement_map.empty()) {
		// check if we have to replace any COUNT aggregates into "CASE WHEN X IS NULL THEN 0 ELSE COUNT END"
		RewriteCountAggregates aggr(replacement_map);
		aggr.VisitOperator(*result.plan);
	}
	return result;
}

bool SubqueryDependentFilter(Expression &expr) {
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_CONJUNCTION &&
	    expr.GetExpressionType() == ExpressionType::CONJUNCTION_AND) {
		auto &bound_conjunction = expr.Cast<BoundConjunctionExpression>();
		for (auto &child : bound_conjunction.children) {
			if (SubqueryDependentFilter(*child)) {
				return true;
			}
		}
	}
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_SUBQUERY) {
		return true;
	}
	return false;
}

FlattenDependentJoins::CorrelatedLayout FlattenDependentJoins::CreateCorrelatedLayout(TableIndex table_index,
                                                                                      idx_t binding_offset,
                                                                                      idx_t correlated_offset) const {
	return CorrelatedLayout::CreateContiguous(ColumnBinding(table_index, ProjectionIndex(binding_offset)),
	                                          correlated_offset, correlated_columns.size());
}

FlattenDependentJoins::CorrelatedLayout FlattenDependentJoins::CreateCorrelatedLayout(TableIndex table_index,
                                                                                      idx_t correlated_offset) const {
	return CreateCorrelatedLayout(table_index, correlated_offset, correlated_offset);
}

FlattenDependentJoins::CorrelatedLayout
FlattenDependentJoins::CreateLeadingCorrelatedLayout(const vector<ColumnBinding> &bindings) const {
	D_ASSERT(bindings.size() >= correlated_columns.size());
	vector<idx_t> correlated_offsets;
	correlated_offsets.reserve(correlated_columns.size());
	vector<ColumnBinding> correlated_bindings;
	correlated_bindings.reserve(correlated_columns.size());
	for (idx_t i = 0; i < correlated_columns.size(); i++) {
		correlated_offsets.push_back(i);
		correlated_bindings.push_back(bindings[i]);
	}
	return CorrelatedLayout(std::move(correlated_bindings), std::move(correlated_offsets));
}

void FlattenDependentJoins::AppendCorrelatedColumns(vector<unique_ptr<Expression>> &expressions,
                                                    const CorrelatedLayout &layout, idx_t count,
                                                    bool include_names) const {
	D_ASSERT(count <= correlated_columns.size());
	for (idx_t i = 0; i < count; i++) {
		auto &col = correlated_columns[i];
		if (include_names) {
			expressions.push_back(make_uniq<BoundColumnRefExpression>(col.name, col.type, layout.GetBinding(i)));
		} else {
			expressions.push_back(make_uniq<BoundColumnRefExpression>(col.type, layout.GetBinding(i)));
		}
	}
}

void FlattenDependentJoins::AddCorrelatedGroupColumns(LogicalAggregate &aggr, const CorrelatedLayout &layout,
                                                      idx_t group_count) const {
	D_ASSERT(group_count <= correlated_columns.size());
	for (idx_t i = 0; i < group_count; i++) {
		auto &col = correlated_columns[i];
		auto colref = make_uniq<BoundColumnRefExpression>(col.name, col.type, layout.GetBinding(i));
		auto new_group_index = ColumnBinding::PushExpression(aggr.groups, std::move(colref));
		for (auto &set : aggr.grouping_sets) {
			set.insert(new_group_index);
		}
	}
}

void FlattenDependentJoins::AddCorrelatedFirstAggregates(LogicalAggregate &aggr, const CorrelatedLayout &layout) const {
	for (idx_t i = 0; i < correlated_columns.size(); i++) {
		auto &col = correlated_columns[i];
		auto first_aggregate = FirstFunctionGetter::GetFunction(col.type);
		auto colref = make_uniq<BoundColumnRefExpression>(col.name, col.type, layout.GetBinding(i));
		vector<unique_ptr<Expression>> aggr_children;
		aggr_children.push_back(std::move(colref));
		auto first_fun = make_uniq<BoundAggregateExpression>(std::move(first_aggregate), std::move(aggr_children),
		                                                     nullptr, nullptr, AggregateType::NON_DISTINCT);
		aggr.expressions.push_back(std::move(first_fun));
	}
}

ColumnBinding FlattenDependentJoins::GetCanonicalBinding(ColumnBinding binding) const {
	auto current = binding;
	while (true) {
		auto entry = equivalent_bindings.find(current);
		if (entry == equivalent_bindings.end() || entry->second == current) {
			return current;
		}
		current = entry->second;
	}
}

void FlattenDependentJoins::RewriteCorrelatedOperator(LogicalOperator &op, const CorrelatedLayout &layout,
                                                      idx_t lateral_depth, bool recursive) {
	RewriteCorrelatedExpressions rewriter(layout.correlated_bindings, correlated_map, lateral_depth, recursive,
	                                      &equivalent_bindings);
	rewriter.VisitOperator(op);
}

FlattenDependentJoins::CorrelatedLayout FlattenDependentJoins::PushDownChild(unique_ptr<LogicalOperator> &child,
                                                                             const PushDownContext &context,
                                                                             CorrelatedLayout layout) {
	auto result = PushDownDependentJoinInternal(std::move(child), context, std::move(layout));
	child = std::move(result.plan);
	return std::move(result.layout);
}

FlattenDependentJoins::CorrelatedLayout FlattenDependentJoins::PushDownChildFresh(unique_ptr<LogicalOperator> &child,
                                                                                  const PushDownContext &context,
                                                                                  CorrelatedLayout layout) {
	auto result = PushDownDependentJoin(std::move(child), context.WithFreshTraversal(), std::move(layout));
	child = std::move(result.plan);
	return std::move(result.layout);
}

FlattenDependentJoins::PushDownResult FlattenDependentJoins::PushDownFilter(unique_ptr<LogicalOperator> plan,
                                                                            PushDownContext context,
                                                                            CorrelatedLayout layout) {
	for (auto &expr : plan->expressions) {
		any_join |= SubqueryDependentFilter(*expr);
	}
	layout = PushDownChild(plan->children[0], context, std::move(layout));

	RewriteCorrelatedOperator(*plan, layout, context.lateral_depth);
	return PushDownResult(std::move(plan), std::move(layout));
}

FlattenDependentJoins::PushDownResult
FlattenDependentJoins::PushDownProjection(unique_ptr<LogicalOperator> plan, PushDownContext context,
                                          CorrelatedLayout layout, bool exit_projection,
                                          unique_ptr<LogicalOperator> delim_scan) {
	auto child_context = context;
	for (auto &expr : plan->expressions) {
		child_context.propagate_null_values &= expr->PropagatesNullValues();
	}

	bool child_is_dependent_join = plan->children[0]->type == LogicalOperatorType::LOGICAL_DEPENDENT_JOIN;
	child_context.propagate_null_values &= !child_is_dependent_join;

	if (exit_projection) {
		auto decorrelated = Decorrelate(std::move(plan->children[0]), child_context.WithFreshTraversal(), layout);
		auto cross_product = LogicalCrossProduct::Create(std::move(decorrelated.plan), std::move(delim_scan));
		if (cross_product->type != LogicalOperatorType::LOGICAL_CROSS_PRODUCT) {
			layout = CreateLeadingCorrelatedLayout(cross_product->GetColumnBindings());
		}
		plan->children[0] = std::move(cross_product);
	} else {
		layout = PushDownChild(plan->children[0], child_context, std::move(layout));
	}

	RewriteCorrelatedOperator(*plan, layout, context.lateral_depth);
	AppendCorrelatedColumns(plan->expressions, layout, correlated_columns.size(), true);
	auto &proj = plan->Cast<LogicalProjection>();
	auto correlated_offset = plan->expressions.size() - correlated_columns.size();
	layout = CreateCorrelatedLayout(proj.table_index, correlated_offset);
	return PushDownResult(std::move(plan), std::move(layout));
}

FlattenDependentJoins::PushDownResult FlattenDependentJoins::PushDownAggregate(unique_ptr<LogicalOperator> plan,
                                                                               PushDownContext context,
                                                                               CorrelatedLayout layout) {
	auto &aggr = plan->Cast<LogicalAggregate>();
	auto child_context = context;
	for (auto &expr : plan->expressions) {
		child_context.propagate_null_values &= expr->PropagatesNullValues();
	}
	layout = PushDownChild(plan->children[0], child_context, std::move(layout));

	RewriteCorrelatedOperator(*plan, layout, context.lateral_depth);

	TableIndex delim_table_index;
	idx_t delim_column_offset;
	auto new_group_count = perform_delim ? correlated_columns.size() : 1;
	AddCorrelatedGroupColumns(aggr, layout, new_group_count);
	if (!perform_delim) {
		delim_table_index = aggr.aggregate_index;
		delim_column_offset = aggr.expressions.size();
		AddCorrelatedFirstAggregates(aggr, layout);
	} else {
		delim_table_index = aggr.group_index;
		delim_column_offset = aggr.groups.size() - correlated_columns.size();
	}
	bool ungrouped_join = false;
	if (aggr.grouping_sets.empty()) {
		ungrouped_join = aggr.groups.size() == new_group_count;
	} else {
		for (auto &grouping_set : aggr.grouping_sets) {
			if (grouping_set.size() == new_group_count) {
				ungrouped_join = true;
			}
		}
	}
	if (ungrouped_join) {
		JoinType join_type = JoinType::INNER;
		if (any_join || !child_context.propagate_null_values) {
			join_type = JoinType::LEFT;
		}
		for (auto &aggr_exp : aggr.expressions) {
			auto &b_aggr_exp = aggr_exp->Cast<BoundAggregateExpression>();
			if (!b_aggr_exp.PropagatesNullValues()) {
				join_type = JoinType::LEFT;
				break;
			}
		}
		unique_ptr<LogicalComparisonJoin> join = make_uniq<LogicalComparisonJoin>(join_type);
		auto left_index = binder.GenerateTableIndex();
		auto delim_scan = make_uniq<LogicalDelimGet>(left_index, delim_types);
		join->children.push_back(std::move(delim_scan));
		join->children.push_back(std::move(plan));
		for (idx_t i = 0; i < new_group_count; i++) {
			auto &col = correlated_columns[i];
			JoinCondition cond(
			    make_uniq<BoundColumnRefExpression>(col.name, col.type, ColumnBinding(left_index, ProjectionIndex(i))),
			    make_uniq<BoundColumnRefExpression>(
			        correlated_columns[i].type,
			        ColumnBinding(delim_table_index, ProjectionIndex(delim_column_offset + i))),
			    ExpressionType::COMPARE_NOT_DISTINCT_FROM);
			join->conditions.push_back(std::move(cond));
		}
		for (idx_t i = 0; i < aggr.expressions.size(); i++) {
			D_ASSERT(aggr.expressions[i]->GetExpressionClass() == ExpressionClass::BOUND_AGGREGATE);
			auto &bound = aggr.expressions[i]->Cast<BoundAggregateExpression>();
			vector<LogicalType> arguments;
			if (bound.function == CountFunctionBase::GetFunction() || bound.function == CountStarFun::GetFunction()) {
				replacement_map[ColumnBinding(aggr.aggregate_index, ProjectionIndex(i))] = i;
			}
		}
		layout = CreateCorrelatedLayout(left_index, 0);
		return PushDownResult(std::move(join), std::move(layout));
	}

	layout = CreateCorrelatedLayout(delim_table_index, delim_column_offset);
	return PushDownResult(std::move(plan), std::move(layout));
}

FlattenDependentJoins::PushDownResult FlattenDependentJoins::PushDownCrossProduct(unique_ptr<LogicalOperator> plan,
                                                                                  PushDownContext context,
                                                                                  CorrelatedLayout layout) {
	D_ASSERT(has_correlated_expressions.find(*plan->children[0]) != has_correlated_expressions.end());
	D_ASSERT(has_correlated_expressions.find(*plan->children[1]) != has_correlated_expressions.end());
	bool left_has_correlation = has_correlated_expressions.find(*plan->children[0])->second;
	bool right_has_correlation = has_correlated_expressions.find(*plan->children[1])->second;
	if (!right_has_correlation) {
		return PushDownSingleCorrelatedChild(std::move(plan), context, std::move(layout), true);
	}
	if (!left_has_correlation) {
		return PushDownSingleCorrelatedChild(std::move(plan), context, std::move(layout), false);
	}

	auto join = make_uniq<LogicalComparisonJoin>(JoinType::INNER);
	auto right_result = PushDownDependentJoinInternal(std::move(plan->children[1]), context, layout);
	plan->children[1] = std::move(right_result.plan);
	auto left_result = PushDownDependentJoinInternal(std::move(plan->children[0]), context, right_result.layout);
	plan->children[0] = std::move(left_result.plan);
	AddComparisonJoinConditions(*join, left_result.layout, right_result.layout);
	join->children.push_back(std::move(plan->children[0]));
	join->children.push_back(std::move(plan->children[1]));
	return PushDownResult(std::move(join), std::move(left_result.layout));
}

FlattenDependentJoins::PushDownResult FlattenDependentJoins::PushDownJoin(unique_ptr<LogicalOperator> plan,
                                                                          PushDownContext context,
                                                                          CorrelatedLayout layout) {
	auto &join = plan->Cast<LogicalJoin>();
	D_ASSERT(plan->children.size() == 2);
	bool left_has_correlation = has_correlated_expressions.find(*plan->children[0])->second;
	bool right_has_correlation = has_correlated_expressions.find(*plan->children[1])->second;

	if (join.join_type == JoinType::INNER) {
		if (!right_has_correlation) {
			return PushDownSingleCorrelatedChild(std::move(plan), context, std::move(layout), true);
		}
		if (!left_has_correlation) {
			return PushDownSingleCorrelatedChild(std::move(plan), context, std::move(layout), false);
		}
	} else if (join.join_type == JoinType::LEFT) {
		if (!right_has_correlation) {
			return PushDownSingleCorrelatedChild(std::move(plan), context, std::move(layout), true);
		}
	} else if (join.join_type == JoinType::RIGHT) {
		if (!left_has_correlation) {
			return PushDownSingleCorrelatedChild(std::move(plan), context, std::move(layout), false);
		}
	} else if (join.join_type == JoinType::MARK) {
		if (!left_has_correlation && right_has_correlation) {
			auto right_result = PushDownDependentJoinInternal(std::move(plan->children[1]), context, layout);
			plan->children[1] = std::move(right_result.plan);

			auto left_result =
			    PushDownDependentJoinInternal(std::move(plan->children[0]), context, right_result.layout);
			plan->children[0] = std::move(left_result.plan);

			AddComparisonJoinConditions(join.Cast<LogicalComparisonJoin>(), left_result.layout, right_result.layout);
			return PushDownResult(std::move(plan), std::move(left_result.layout));
		}

		auto left_result = PushDownDependentJoinInternal(std::move(plan->children[0]), context, layout);
		plan->children[0] = std::move(left_result.plan);
		plan->children[1] = DecorrelateIndependent(binder, std::move(plan->children[1]));
		RewriteCorrelatedOperator(*plan, left_result.layout, context.lateral_depth);
		return PushDownResult(std::move(plan), std::move(left_result.layout));
	} else {
		throw NotImplementedException("Unsupported join type for flattening correlated subquery");
	}

	auto left_result = PushDownDependentJoinInternal(std::move(plan->children[0]), context, layout);
	plan->children[0] = std::move(left_result.plan);
	auto right_result = PushDownDependentJoinInternal(std::move(plan->children[1]), context, left_result.layout);
	plan->children[1] = std::move(right_result.plan);
	CorrelatedLayout result_layout = right_result.layout;
	if (join.join_type == JoinType::LEFT) {
		result_layout = left_result.layout;
	} else if (join.join_type == JoinType::RIGHT) {
		result_layout.ShiftOffsets(plan->children[0]->GetColumnBindings().size());
	}
	AddCorrelatedJoinConditions(join, left_result.layout, right_result.layout);
	RewriteCorrelatedOperator(*plan, right_result.layout, context.lateral_depth);
	return PushDownResult(std::move(plan), std::move(result_layout));
}

FlattenDependentJoins::PushDownResult FlattenDependentJoins::PushDownLimit(unique_ptr<LogicalOperator> plan,
                                                                           PushDownContext context,
                                                                           CorrelatedLayout layout) {
	auto &limit = plan->Cast<LogicalLimit>();
	switch (limit.limit_val.Type()) {
	case LimitNodeType::CONSTANT_PERCENTAGE:
	case LimitNodeType::EXPRESSION_PERCENTAGE:
		throw ParserException("Limit percent operator not supported in correlated subquery");
	case LimitNodeType::EXPRESSION_VALUE:
		throw ParserException("Non-constant limit not supported in correlated subquery");
	default:
		break;
	}
	switch (limit.offset_val.Type()) {
	case LimitNodeType::EXPRESSION_VALUE:
		throw ParserException("Non-constant offset not supported in correlated subquery");
	case LimitNodeType::CONSTANT_PERCENTAGE:
	case LimitNodeType::EXPRESSION_PERCENTAGE:
		throw InternalException("Percentage offset in FlattenDependentJoin");
	default:
		break;
	}
	auto rownum_alias = "limit_rownum";
	unique_ptr<LogicalOperator> child;
	unique_ptr<LogicalOrder> order_by;

	if (plan->children[0]->type == LogicalOperatorType::LOGICAL_ORDER_BY) {
		order_by = unique_ptr_cast<LogicalOperator, LogicalOrder>(std::move(plan->children[0]));
		layout = PushDownChild(order_by->children[0], context, std::move(layout));
		child = std::move(order_by->children[0]);
	} else {
		layout = PushDownChild(plan->children[0], context, std::move(layout));
		child = std::move(plan->children[0]);
	}
	auto child_column_count = child->GetColumnBindings().size();
	auto window_index = binder.GenerateTableIndex();
	auto window = make_uniq<LogicalWindow>(window_index);
	auto rn = make_uniq<WindowFunction>(RowNumberFun::GetFunction());
	auto row_number = make_uniq<BoundWindowExpression>(LogicalType::BIGINT, nullptr, std::move(rn), nullptr);
	auto partition_count = perform_delim ? correlated_columns.size() : 1;
	AppendCorrelatedColumns(row_number->partitions, layout, partition_count, true);
	if (order_by) {
		row_number->orders = std::move(order_by->orders);
	}
	row_number->start = WindowBoundary::UNBOUNDED_PRECEDING;
	row_number->end = WindowBoundary::CURRENT_ROW_ROWS;
	window->expressions.push_back(std::move(row_number));
	window->children.push_back(std::move(child));

	auto filter = make_uniq<LogicalFilter>();
	unique_ptr<Expression> condition;
	auto row_num_ref = make_uniq<BoundColumnRefExpression>(rownum_alias, LogicalType::BIGINT,
	                                                       ColumnBinding(window_index, ProjectionIndex(0)));

	if (limit.limit_val.Type() == LimitNodeType::CONSTANT_VALUE) {
		auto upper_bound_limit = NumericLimits<int64_t>::Maximum();
		auto limit_val = int64_t(limit.limit_val.GetConstantValue());
		if (limit.offset_val.Type() == LimitNodeType::CONSTANT_VALUE) {
			auto offset_val = int64_t(limit.offset_val.GetConstantValue());
			TryAddOperator::Operation(limit_val, offset_val, upper_bound_limit);
		} else {
			upper_bound_limit = limit_val;
		}
		auto upper_bound = make_uniq<BoundConstantExpression>(Value::BIGINT(upper_bound_limit));
		condition = make_uniq<BoundComparisonExpression>(ExpressionType::COMPARE_LESSTHANOREQUALTO, row_num_ref->Copy(),
		                                                 std::move(upper_bound));
	}
	if (limit.offset_val.Type() == LimitNodeType::CONSTANT_VALUE) {
		auto offset_val = int64_t(limit.offset_val.GetConstantValue());
		auto lower_bound = make_uniq<BoundConstantExpression>(Value::BIGINT(offset_val));
		auto lower_comp = make_uniq<BoundComparisonExpression>(ExpressionType::COMPARE_GREATERTHAN, row_num_ref->Copy(),
		                                                       std::move(lower_bound));
		if (condition) {
			auto conj = make_uniq<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_AND, std::move(lower_comp),
			                                                  std::move(condition));
			condition = std::move(conj);
		} else {
			condition = std::move(lower_comp);
		}
	}
	filter->expressions.push_back(std::move(condition));
	filter->children.push_back(std::move(window));
	for (idx_t i = 0; i < child_column_count; i++) {
		filter->projection_map.emplace_back(i);
	}
	return PushDownResult(std::move(filter), std::move(layout));
}

FlattenDependentJoins::PushDownResult FlattenDependentJoins::PushDownWindow(unique_ptr<LogicalOperator> plan,
                                                                            PushDownContext context,
                                                                            CorrelatedLayout layout) {
	auto &window = plan->Cast<LogicalWindow>();
	layout = PushDownChild(plan->children[0], context, std::move(layout));

	RewriteCorrelatedOperator(*plan, layout, context.lateral_depth);

	for (auto &expr : window.expressions) {
		D_ASSERT(expr->GetExpressionClass() == ExpressionClass::BOUND_WINDOW);
		auto &w = expr->Cast<BoundWindowExpression>();
		AppendCorrelatedColumns(w.partitions, layout, correlated_columns.size(), false);
	}
	return PushDownResult(std::move(plan), std::move(layout));
}

FlattenDependentJoins::PushDownResult FlattenDependentJoins::PushDownSetOperation(unique_ptr<LogicalOperator> plan,
                                                                                  PushDownContext context,
                                                                                  CorrelatedLayout layout) {
	auto &setop = plan->Cast<LogicalSetOperation>();
#ifdef DEBUG
	for (auto &child : plan->children) {
		child->ResolveOperatorTypes();
	}
	for (idx_t i = 1; i < plan->children.size(); i++) {
		D_ASSERT(plan->children[0]->types.size() == plan->children[i]->types.size());
	}
#endif
	for (auto &child : plan->children) {
		layout = PushDownChildFresh(child, context, std::move(layout));
	}
	for (idx_t i = 0; i < plan->children.size(); i++) {
		if (plan->children[i]->type == LogicalOperatorType::LOGICAL_CROSS_PRODUCT) {
			auto proj_index = binder.GenerateTableIndex();
			auto bindings = plan->children[i]->GetColumnBindings();
			plan->children[i]->ResolveOperatorTypes();
			auto types = plan->children[i]->types;
			vector<unique_ptr<Expression>> expressions;
			expressions.reserve(bindings.size());
			D_ASSERT(bindings.size() == types.size());

			for (idx_t col_idx = 0; col_idx < bindings.size(); col_idx++) {
				expressions.push_back(make_uniq<BoundColumnRefExpression>(types[col_idx], bindings[col_idx]));
			}
			auto proj = make_uniq<LogicalProjection>(proj_index, std::move(expressions));
			proj->children.push_back(std::move(plan->children[i]));
			plan->children[i] = std::move(proj);
		}
	}

#ifdef DEBUG
	for (idx_t i = 1; i < plan->children.size(); i++) {
		D_ASSERT(plan->children[0]->GetColumnBindings().size() == plan->children[i]->GetColumnBindings().size());
	}
	for (auto &child : plan->children) {
		child->ResolveOperatorTypes();
	}
	for (idx_t i = 1; i < plan->children.size(); i++) {
		D_ASSERT(plan->children[0]->types.size() == plan->children[i]->types.size());
	}
#endif
	layout = CreateCorrelatedLayout(setop.table_index, setop.column_count);
	setop.column_count += correlated_columns.size();
	return PushDownResult(std::move(plan), std::move(layout));
}

FlattenDependentJoins::PushDownResult FlattenDependentJoins::PushDownDistinct(unique_ptr<LogicalOperator> plan,
                                                                              PushDownContext context,
                                                                              CorrelatedLayout layout) {
	auto &distinct = plan->Cast<LogicalDistinct>();
	layout = PushDownChildFresh(distinct.children[0], context, std::move(layout));
	AppendCorrelatedColumns(distinct.distinct_targets, layout, correlated_columns.size(), false);
	return PushDownResult(std::move(plan), std::move(layout));
}

FlattenDependentJoins::PushDownResult FlattenDependentJoins::PushDownExpressionGet(unique_ptr<LogicalOperator> plan,
                                                                                   PushDownContext context,
                                                                                   CorrelatedLayout layout) {
	layout = PushDownChild(plan->children[0], context, std::move(layout));

	RewriteCorrelatedOperator(*plan, layout, context.lateral_depth);

	auto &expr_get = plan->Cast<LogicalExpressionGet>();
	for (auto &expr_list : expr_get.expressions) {
		AppendCorrelatedColumns(expr_list, layout, correlated_columns.size(), false);
	}
	for (idx_t i = 0; i < correlated_columns.size(); i++) {
		expr_get.expr_types.push_back(correlated_columns[i].type);
	}
	auto correlated_offset = expr_get.expr_types.size() - correlated_columns.size();
	layout = CreateCorrelatedLayout(expr_get.table_index, correlated_offset);
	return PushDownResult(std::move(plan), std::move(layout));
}

FlattenDependentJoins::PushDownResult FlattenDependentJoins::PushDownOrderBy(unique_ptr<LogicalOperator> plan,
                                                                             PushDownContext context,
                                                                             CorrelatedLayout layout) {
	layout = PushDownChildFresh(plan->children[0], context, std::move(layout));
	return PushDownResult(std::move(plan), std::move(layout));
}

FlattenDependentJoins::PushDownResult
FlattenDependentJoins::PushDownGet(unique_ptr<LogicalOperator> plan, PushDownContext context, CorrelatedLayout layout) {
	auto &get = plan->Cast<LogicalGet>();
	if (get.children.size() != 1) {
		throw InternalException("Flatten dependent joins - logical get encountered without children");
	}
	layout = PushDownChildFresh(plan->children[0], context, std::move(layout));
	auto correlated_offset = get.GetColumnBindings().size();
	for (idx_t i = 0; i < correlated_columns.size(); i++) {
		get.projected_input.push_back(layout.GetOffset(i));
	}

	RewriteCorrelatedOperator(*plan, layout, context.lateral_depth);
	layout.correlated_offsets.clear();
	for (idx_t i = 0; i < correlated_columns.size(); i++) {
		layout.correlated_offsets.push_back(correlated_offset + i);
	}
	return PushDownResult(std::move(plan), std::move(layout));
}

FlattenDependentJoins::PushDownResult
FlattenDependentJoins::PushDownCTE(unique_ptr<LogicalOperator> plan, PushDownContext context, CorrelatedLayout layout) {
#ifdef DEBUG
	plan->children[0]->ResolveOperatorTypes();
	plan->children[1]->ResolveOperatorTypes();
#endif
	if (plan->type == LogicalOperatorType::LOGICAL_MATERIALIZED_CTE) {
		bool left_has_correlation = has_correlated_expressions.find(*plan->children[0])->second;
		bool right_has_correlation = has_correlated_expressions.find(*plan->children[1])->second;

		if (!left_has_correlation) {
			if (right_has_correlation) {
				layout = PushDownChild(plan->children[1], context, std::move(layout));
				plan->children[0] = DecorrelateIndependent(binder, std::move(plan->children[0]));
			}
			return PushDownResult(std::move(plan), std::move(layout));
		}
	}

	layout = PushDownChild(plan->children[0], context, std::move(layout));

	auto &setop = plan->Cast<LogicalCTE>();
	auto table_index = setop.table_index;
	setop.correlated_columns = correlated_columns;
	binder.recursive_ctes[setop.table_index] = &setop;
	layout = CreateCorrelatedLayout(setop.table_index, setop.column_count);

	if (plan->type == LogicalOperatorType::LOGICAL_RECURSIVE_CTE) {
		auto &rec_cte = plan->Cast<LogicalRecursiveCTE>();

		if (!rec_cte.key_targets.empty()) {
			AppendCorrelatedColumns(rec_cte.key_targets, layout, correlated_columns.size(), false);
		}
		for (idx_t i = 0; i < correlated_columns.size(); i++) {
			rec_cte.internal_types.push_back(correlated_columns[i].type);
		}
	}

	RewriteCTEScan::Rewrite(*plan->children[1], table_index, correlated_columns);
	DetectCorrelatedExpressions(*plan->children[1], false, 0);

	layout = PushDownChild(plan->children[1], context.WithPropagateNullValues(false), std::move(layout));

	RewriteCorrelatedOperator(*plan, layout, context.lateral_depth);
	RewriteCorrelatedOperator(*plan->children[0], layout, context.lateral_depth + 1, true);
	RewriteCorrelatedOperator(*plan->children[1], layout, context.lateral_depth + 1, true);

#ifdef DEBUG
	plan->children[0]->ResolveOperatorTypes();
	plan->children[1]->ResolveOperatorTypes();
#endif

	if (plan->type == LogicalOperatorType::LOGICAL_RECURSIVE_CTE) {
		layout = CreateCorrelatedLayout(setop.table_index, setop.column_count);
	}

	setop.column_count += correlated_columns.size();
	return PushDownResult(std::move(plan), std::move(layout));
}

FlattenDependentJoins::PushDownResult FlattenDependentJoins::PushDownCTERef(unique_ptr<LogicalOperator> plan,
                                                                            PushDownContext context,
                                                                            CorrelatedLayout layout) {
	auto &cteref = plan->Cast<LogicalCTERef>();
	auto correlated_offset = cteref.chunk_types.size() - cteref.correlated_columns;
	layout = CreateCorrelatedLayout(cteref.table_index, correlated_offset);
	return PushDownResult(std::move(plan), std::move(layout));
}

FlattenDependentJoins::PushDownResult
FlattenDependentJoins::PushDownDependentJoinInternal(unique_ptr<LogicalOperator> plan, PushDownContext context,
                                                     CorrelatedLayout layout) {
	// first check if the logical operator has correlated expressions
	auto entry = has_correlated_expressions.find(*plan);
	bool exit_projection = false;
	unique_ptr<LogicalOperator> delim_scan;
	D_ASSERT(entry != has_correlated_expressions.end());
	if (!entry->second) {
		// we reached a node without correlated expressions
		// we can eliminate the dependent join now and create a simple cross product
		// now create the duplicate eliminated scan for this node
		if (plan->type == LogicalOperatorType::LOGICAL_CTE_REF) {
			auto &op = plan->Cast<LogicalCTERef>();

			auto rec_cte = binder.recursive_ctes.find(op.cte_index);
			if (rec_cte != binder.recursive_ctes.end()) {
				D_ASSERT(rec_cte->second->type == LogicalOperatorType::LOGICAL_RECURSIVE_CTE ||
				         rec_cte->second->type == LogicalOperatorType::LOGICAL_MATERIALIZED_CTE);

				auto &rec_cte_op = rec_cte->second->Cast<LogicalCTE>();
				if (op.correlated_columns == 0) {
					RewriteCTEScan::Rewrite(*plan, op.cte_index, rec_cte_op.correlated_columns);
				}
			}
		}

		// create cross product with Delim Join
		auto delim_index = binder.GenerateTableIndex();
		auto left_columns = plan->GetColumnBindings().size();
		layout = CreateCorrelatedLayout(delim_index, 0, left_columns);
		delim_scan = make_uniq<LogicalDelimGet>(delim_index, delim_types);
		if (plan->type == LogicalOperatorType::LOGICAL_PROJECTION) {
			// we want to keep the logical projection for positionality.
			exit_projection = true;
		} else if (plan->type == LogicalOperatorType::LOGICAL_CTE_REF) {
			// Should a reference to a CTE be the final non-recursive operator,
			// we have to add a filter predicate to ensure column equality between
			// the left and right side of the join. A simple cross product does not
			// suffice in this case.
			auto &cteref = plan->Cast<LogicalCTERef>();
			auto join = make_uniq<LogicalComparisonJoin>(JoinType::INNER);
			AddCTERefJoinConditions(*join, cteref, layout);
			if (!join->conditions.empty()) {
				join->children.push_back(std::move(plan));
				join->children.push_back(std::move(delim_scan));
				return PushDownResult(std::move(join), std::move(layout));
			}

			auto decorrelated = Decorrelate(std::move(plan), context.WithFreshTraversal(), layout);
			auto cross_product = LogicalCrossProduct::Create(std::move(decorrelated.plan), std::move(delim_scan));
			auto result_layout = layout;
			if (cross_product->type != LogicalOperatorType::LOGICAL_CROSS_PRODUCT) {
				result_layout = CreateLeadingCorrelatedLayout(cross_product->GetColumnBindings());
			}
			return PushDownResult(std::move(cross_product), std::move(result_layout));
		} else {
			auto decorrelated = Decorrelate(std::move(plan), context.WithFreshTraversal(), layout);
			auto cross_product = LogicalCrossProduct::Create(std::move(decorrelated.plan), std::move(delim_scan));
			auto result_layout = layout;
			if (cross_product->type != LogicalOperatorType::LOGICAL_CROSS_PRODUCT) {
				result_layout = CreateLeadingCorrelatedLayout(cross_product->GetColumnBindings());
			}
			return PushDownResult(std::move(cross_product), std::move(result_layout));
		}
	}
	switch (plan->type) {
	case LogicalOperatorType::LOGICAL_UNNEST:
	case LogicalOperatorType::LOGICAL_FILTER: {
		return PushDownFilter(std::move(plan), context, std::move(layout));
	}
	case LogicalOperatorType::LOGICAL_PROJECTION: {
		return PushDownProjection(std::move(plan), context, std::move(layout), exit_projection, std::move(delim_scan));
	}
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
		return PushDownAggregate(std::move(plan), context, std::move(layout));
	}
	case LogicalOperatorType::LOGICAL_CROSS_PRODUCT: {
		return PushDownCrossProduct(std::move(plan), context, std::move(layout));
	}
	case LogicalOperatorType::LOGICAL_DEPENDENT_JOIN: {
		D_ASSERT(plan->children.size() == 2);
		return Decorrelate(std::move(plan), context, std::move(layout));
	}
	case LogicalOperatorType::LOGICAL_ANY_JOIN:
	case LogicalOperatorType::LOGICAL_ASOF_JOIN:
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN: {
		return PushDownJoin(std::move(plan), context, std::move(layout));
	}
	case LogicalOperatorType::LOGICAL_LIMIT: {
		return PushDownLimit(std::move(plan), context, std::move(layout));
	}
	case LogicalOperatorType::LOGICAL_WINDOW: {
		return PushDownWindow(std::move(plan), context, std::move(layout));
	}
	case LogicalOperatorType::LOGICAL_EXCEPT:
	case LogicalOperatorType::LOGICAL_INTERSECT:
	case LogicalOperatorType::LOGICAL_UNION: {
		return PushDownSetOperation(std::move(plan), context, std::move(layout));
	}
	case LogicalOperatorType::LOGICAL_DISTINCT: {
		return PushDownDistinct(std::move(plan), context, std::move(layout));
	}
	case LogicalOperatorType::LOGICAL_EXPRESSION_GET: {
		return PushDownExpressionGet(std::move(plan), context, std::move(layout));
	}
	case LogicalOperatorType::LOGICAL_PIVOT:
		throw BinderException("PIVOT is not supported in correlated subqueries yet");
	case LogicalOperatorType::LOGICAL_ORDER_BY: {
		return PushDownOrderBy(std::move(plan), context, std::move(layout));
	}
	case LogicalOperatorType::LOGICAL_GET: {
		return PushDownGet(std::move(plan), context, std::move(layout));
	}
	case LogicalOperatorType::LOGICAL_MATERIALIZED_CTE:
	case LogicalOperatorType::LOGICAL_RECURSIVE_CTE: {
		return PushDownCTE(std::move(plan), context, std::move(layout));
	}
	case LogicalOperatorType::LOGICAL_CTE_REF: {
		return PushDownCTERef(std::move(plan), context, std::move(layout));
	}
	case LogicalOperatorType::LOGICAL_DELIM_JOIN: {
		throw BinderException("Nested lateral joins or lateral joins in correlated subqueries are not (yet) supported");
	}
	case LogicalOperatorType::LOGICAL_SAMPLE:
		throw BinderException("Sampling in correlated subqueries is not (yet) supported");
	case LogicalOperatorType::LOGICAL_POSITIONAL_JOIN:
		throw BinderException("Positional join in correlated subqueries is not (yet) supported");
	default:
		throw InternalException("Logical operator type \"%s\" for dependent join", LogicalOperatorToString(plan->type));
	}
}

} // namespace duckdb
