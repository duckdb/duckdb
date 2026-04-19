#include "duckdb/planner/subquery/flatten_dependent_join.hpp"

#include "duckdb/common/operator/add.hpp"
#include "duckdb/common/exception/parser_exception.hpp"
#include "duckdb/execution/column_binding_resolver.hpp"
#include "duckdb/function/aggregate/distributive_functions.hpp"
#include "duckdb/function/aggregate/distributive_function_utils.hpp"
#include "duckdb/function/window/rows_functions.hpp"
#include "duckdb/optimizer/column_binding_replacer.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/list.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/list.hpp"
#include "duckdb/planner/subquery/rewrite_correlated_expressions.hpp"
#include "duckdb/planner/operator/logical_dependent_join.hpp"

namespace duckdb {

static bool HasCTEAccessor(LogicalOperator &op, TableIndex table_index) {
	if (op.type == LogicalOperatorType::LOGICAL_CTE_REF) {
		if (op.Cast<LogicalCTERef>().cte_index == table_index) {
			return true;
		}
	}
	for (auto &child : op.children) {
		if (HasCTEAccessor(*child, table_index)) {
			return true;
		}
	}
	return false;
}

static void RemapLocalBindings(LogicalOperator &op, const vector<ColumnBinding> &old_bindings,
                               const vector<ColumnBinding> &new_bindings) {
	if (old_bindings == new_bindings) {
		return;
	}
	vector<ColumnBinding> unmatched_old_bindings;
	vector<ColumnBinding> unmatched_new_bindings;
	for (auto &old_b : old_bindings) {
		if (std::find(new_bindings.begin(), new_bindings.end(), old_b) == new_bindings.end()) {
			unmatched_old_bindings.push_back(old_b);
		}
	}
	for (auto &new_b : new_bindings) {
		if (std::find(old_bindings.begin(), old_bindings.end(), new_b) == old_bindings.end()) {
			unmatched_new_bindings.push_back(new_b);
		}
	}
	D_ASSERT(unmatched_old_bindings.size() <= unmatched_new_bindings.size());
	if (unmatched_old_bindings.empty()) {
		return;
	}
	ColumnBindingReplacer replacer;
	for (idx_t i = 0; i < unmatched_old_bindings.size(); i++) {
		replacer.replacement_bindings.emplace_back(unmatched_old_bindings[i], unmatched_new_bindings[i]);
	}
	LogicalOperatorVisitor::EnumerateExpressions(
	    op, [&](unique_ptr<Expression> *expression) { replacer.VisitExpression(expression); });
}

static bool DependsOnCorrelatedWalk(LogicalOperator &op, Binder &binder,
                                    const column_binding_map_t<ColumnBinding> &correlated_aliases,
                                    reference_map_t<LogicalOperator, bool> &cache) {
	auto it = cache.find(op);
	if (it != cache.end()) {
		return it->second;
	}
	bool result = false;
	// check CTE_REF nodes for structurally-propagated correlation
	if (op.type == LogicalOperatorType::LOGICAL_CTE_REF) {
		auto &cteref = op.Cast<LogicalCTERef>();
		if (cteref.correlated_columns > 0) {
			auto rec_cte = binder.recursive_ctes.find(cteref.cte_index);
			if (rec_cte != binder.recursive_ctes.end()) {
				auto &cte_corr_cols = rec_cte->second->Cast<LogicalCTE>().correlated_columns;
				auto cte_corr_start = cte_corr_cols.size() - cteref.correlated_columns;
				for (idx_t i = cte_corr_start; i < cte_corr_cols.size(); i++) {
					if (correlated_aliases.find(cte_corr_cols[i].binding) != correlated_aliases.end()) {
						result = true;
						break;
					}
				}
			}
		}
	}
	if (!result) {
		// check expressions for depth > 0 column references
		LogicalOperatorVisitor::EnumerateExpressions(op, [&](unique_ptr<Expression> *expr_ptr) {
			if (result) {
				return;
			}
			ExpressionIterator::VisitExpression<BoundColumnRefExpression>(
			    **expr_ptr, [&](const BoundColumnRefExpression &bound_colref) {
				    result |= bound_colref.depth > 0 &&
				              correlated_aliases.find(bound_colref.binding) != correlated_aliases.end();
			    });
		});
	}
	if (!result) {
		// recurse into children
		for (auto &child : op.children) {
			if (DependsOnCorrelatedWalk(*child, binder, correlated_aliases, cache)) {
				result = true;
				break;
			}
		}
	}
	cache.emplace(op, result);
	return result;
}

FlattenDependentJoins::FlattenDependentJoins(Binder &binder, const CorrelatedColumns &correlated, bool perform_delim,
                                             bool any_join, optional_ptr<FlattenDependentJoins> parent)
    : binder(binder), correlated_columns(correlated), perform_delim(perform_delim), any_join(any_join), parent(parent) {
	correlated_base_bindings.reserve(correlated_columns.size());
	for (idx_t i = 0; i < correlated_columns.size(); i++) {
		auto &col = correlated_columns[i];
		ColumnBinding base_binding = col.binding;
		if (parent) {
			auto parent_base = parent->GetCorrelatedBase(col.binding);
			if (parent_base) {
				base_binding = *parent_base;
			}
		}
		correlated_base_bindings.push_back(base_binding);
		correlated_aliases[col.binding] = base_binding;
		delim_types.push_back(col.type);
	}
	if (parent) {
		MergeCorrelatedAliases(*parent);
	}
}

vector<ColumnBinding> FlattenDependentJoins::CreateContiguousState(ColumnBinding base_binding) const {
	vector<ColumnBinding> state;
	state.reserve(correlated_columns.size());
	for (idx_t i = 0; i < correlated_columns.size(); i++) {
		state.push_back(ColumnBinding(base_binding.table_index, ProjectionIndex(base_binding.column_index + i)));
	}
	return state;
}

bool FlattenDependentJoins::DependsOnCorrelated(LogicalOperator &op) const {
	return DependsOnCorrelatedWalk(op, binder, correlated_aliases, dependency_cache);
}

optional_ptr<const ColumnBinding> FlattenDependentJoins::GetCorrelatedBase(const ColumnBinding &binding) const {
	auto entry = correlated_aliases.find(binding);
	if (entry == correlated_aliases.end()) {
		return nullptr;
	}
	return entry->second;
}

optional_idx FlattenDependentJoins::GetCorrelatedIndexByBase(const ColumnBinding &base_binding) const {
	for (idx_t i = 0; i < correlated_base_bindings.size(); i++) {
		if (correlated_base_bindings[i] == base_binding) {
			return i;
		}
	}
	return optional_idx();
}

optional_idx FlattenDependentJoins::GetCorrelatedIndex(const ColumnBinding &binding) const {
	auto base_binding = GetCorrelatedBase(binding);
	if (!base_binding) {
		return optional_idx();
	}
	return GetCorrelatedIndexByBase(*base_binding);
}

void FlattenDependentJoins::MergeCorrelatedAliases(const FlattenDependentJoins &source) {
	for (auto &entry : source.correlated_aliases) {
		if (!GetCorrelatedIndexByBase(entry.second).IsValid()) {
			continue;
		}
		auto result = correlated_aliases.emplace(entry);
		D_ASSERT(result.second || result.first->second == entry.second);
	}
}

idx_t FlattenDependentJoins::GetDelimKeyIndex(idx_t index) const {
	D_ASSERT(index < (perform_delim ? correlated_columns.size() : 1));
	if (perform_delim) {
		return index;
	}
	auto delim_index = correlated_columns.GetDelimIndex();
	D_ASSERT(delim_index < correlated_columns.size());
	return delim_index;
}

void FlattenDependentJoins::PatchAccessingOperators(LogicalOperator &subtree_root, TableIndex table_index,
                                                    const CorrelatedColumns &correlated_columns) {
	// patch CTE_REF nodes that access the given CTE
	if (subtree_root.type == LogicalOperatorType::LOGICAL_CTE_REF) {
		auto &reader = subtree_root.Cast<LogicalCTERef>();
		if (reader.cte_index == table_index && reader.correlated_columns == 0) {
			for (auto &column : correlated_columns) {
				reader.chunk_types.push_back(column.type);
				reader.bound_columns.push_back(column.name);
			}
			reader.correlated_columns += correlated_columns.size();
		}
	}
	// patch DEPENDENT_JOIN nodes that have a child accessing the given CTE
	if (subtree_root.type == LogicalOperatorType::LOGICAL_DEPENDENT_JOIN) {
		auto &join = subtree_root.Cast<LogicalDependentJoin>();
		bool has_cte_ref_child = false;
		for (auto &child : join.children) {
			if (HasCTEAccessor(*child, table_index)) {
				has_cte_ref_child = true;
				break;
			}
		}
		if (has_cte_ref_child) {
			for (auto &column : correlated_columns) {
				bool found = std::find(join.correlated_columns.begin(), join.correlated_columns.end(), column) !=
				             join.correlated_columns.end();
				if (!found) {
					join.correlated_columns.AddColumnToBack(column);
				}
			}
		}
	}
	// recurse into children
	for (auto &child : subtree_root.children) {
		PatchAccessingOperators(*child, table_index, correlated_columns);
	}
}

void FlattenDependentJoins::CreateDelimJoinConditions(LogicalComparisonJoin &delim_join,
                                                      const CorrelatedColumns &correlated_columns,
                                                      const vector<ColumnBinding> &state, bool perform_delim) {
	D_ASSERT(state.size() == correlated_columns.size());
	auto key_count = perform_delim ? correlated_columns.size() : 1;
	for (idx_t i = 0; i < key_count; i++) {
		auto delim_index = perform_delim ? i : correlated_columns.GetDelimIndex();
		D_ASSERT(delim_index < correlated_columns.size());
		auto &col = correlated_columns[delim_index];
		JoinCondition cond(make_uniq<BoundColumnRefExpression>(col.name, col.type, col.binding),
		                   make_uniq<BoundColumnRefExpression>(col.name, col.type, state[delim_index]),
		                   ExpressionType::COMPARE_NOT_DISTINCT_FROM);
		delim_join.conditions.push_back(std::move(cond));
	}
}

void FlattenDependentJoins::PopulateDuplicateEliminatedColumns(LogicalDependentJoin &op) {
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
	flatten.Decorrelate(plan);
	return std::move(plan);
}

vector<ColumnBinding> FlattenDependentJoins::Decorrelate(unique_ptr<LogicalOperator> &plan, bool propagate_null_values,
                                                         vector<ColumnBinding> state) {
	switch (plan->type) {
	case LogicalOperatorType::LOGICAL_DEPENDENT_JOIN: {
		return DecorrelateDependentJoin(plan, propagate_null_values, std::move(state));
	}
	default: {
		for (auto &child : plan->children) {
			auto old_child_bindings = child->GetColumnBindings();
			state = Decorrelate(child, propagate_null_values, std::move(state));
			RemapLocalBindings(*plan, old_child_bindings, child->GetColumnBindings());
		}
	}
	}

	return std::move(state);
}

vector<ColumnBinding> FlattenDependentJoins::DecorrelateDependentJoin(unique_ptr<LogicalOperator> &plan,
                                                                      bool propagate_null_values,
                                                                      vector<ColumnBinding> state) {
	auto &op = plan->Cast<LogicalDependentJoin>();
	auto left_child_has_correlation = DependsOnCorrelated(*plan->children[0]);
	state = PrepareDependentJoinLeft(op, propagate_null_values, std::move(state));

	FlattenDependentJoins flatten(binder, op.correlated_columns, op.perform_delim, op.any_join, this);

	unique_ptr<LogicalOperator> cte = nullptr;

	// 1. Detect and bypass CTE if the left side is uncorrelated
	if (plan->children[1]->type == LogicalOperatorType::LOGICAL_MATERIALIZED_CTE) {
		auto &cte_ref = plan->children[1]->Cast<LogicalMaterializedCTE>();

		if (!flatten.DependsOnCorrelated(*cte_ref.children[0])) {
			// Temporarily pull the CTE node out of the tree
			cte = std::move(plan->children[1]);
			plan->children[1] = std::move(cte->children[1]);

			// Guard Clause: If we need a full decorrelation here, do it and exit early
			if (left_child_has_correlation || op.join_type != JoinType::SINGLE) {
				state = Decorrelate(plan, propagate_null_values, std::move(state));
				cte->children[1] = std::move(plan);
				plan = std::move(cte);
				return state;
			}
		}
	}

	auto flatten_state = flatten.PushDownDependentJoin(plan->children[1], op.propagate_null_values);
	MergeCorrelatedAliases(flatten);

	if (!parent && !correlated_columns.empty()) {
		state = flatten_state;
	}

	state = FinalizeDependentJoin(plan, std::move(state), flatten_state);

	// Re-attach the CTE if we extracted one earlier
	if (cte) {
		cte->children[1] = std::move(plan);
		plan = std::move(cte);
	}

	return state;
}

// General-purpose Row Number Window Builder
static unique_ptr<LogicalWindow> CreateRowNumberWindow(unique_ptr<LogicalOperator> child, TableIndex table_index,
                                                       vector<unique_ptr<Expression>> partitions = {},
                                                       vector<BoundOrderByNode> orders = {}) {
	auto window = make_uniq<LogicalWindow>(table_index);
	auto rn_func = make_uniq<WindowFunction>(RowNumberFun::GetFunction());
	auto row_number = make_uniq<BoundWindowExpression>(LogicalType::BIGINT, nullptr, std::move(rn_func), nullptr);

	row_number->partitions = std::move(partitions);
	row_number->orders = std::move(orders);
	row_number->start = WindowBoundary::UNBOUNDED_PRECEDING;
	row_number->end = WindowBoundary::CURRENT_ROW_ROWS;
	row_number->SetAlias("limit_rownum");

	window->expressions.push_back(std::move(row_number));
	window->AddChild(std::move(child));
	return window;
}

vector<ColumnBinding> FlattenDependentJoins::PrepareDependentJoinLeft(LogicalDependentJoin &op,
                                                                      bool propagate_null_values,
                                                                      vector<ColumnBinding> state) {
	// If we have a parent, we unnest the left side of the DEPENDENT JOIN in the parent's context.
	if (parent) {
		// only push the dependent join to the left side, if there is correlation.
		if (DependsOnCorrelated(op)) {
			state = PushDownDependentJoin(op.children[0], propagate_null_values, std::move(state));
		} else {
			// There might be unrelated correlation, so we have to traverse the tree
			op.children[0] = DecorrelateIndependent(binder, std::move(op.children[0]));
		}

		RewriteCorrelatedExpressions::Rewrite(op, correlated_base_bindings, state, correlated_aliases);
	} else {
		state = Decorrelate(op.children[0], true, std::move(state));
	}

	if (!op.perform_delim) {
		// if we are not performing a delim join, we push a row_number() OVER() window operator on the LHS
		// and perform all duplicate elimination on that row number instead
		const auto &op_col = op.correlated_columns[op.correlated_columns.GetDelimIndex()];
		op.children[0] = CreateRowNumberWindow(std::move(op.children[0]), op_col.binding.table_index);
	}
	return state;
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
                                                        const vector<ColumnBinding> &left_state,
                                                        const vector<ColumnBinding> &right_state) const {
	for (idx_t i = 0; i < correlated_columns.size(); i++) {
		JoinCondition cond(make_uniq<BoundColumnRefExpression>(correlated_columns[i].type, left_state[i]),
		                   make_uniq<BoundColumnRefExpression>(correlated_columns[i].type, right_state[i]),
		                   ExpressionType::COMPARE_NOT_DISTINCT_FROM);
		join.conditions.push_back(std::move(cond));
	}
}

void FlattenDependentJoins::AddCTERefJoinConditions(LogicalComparisonJoin &join, const LogicalCTERef &cteref,
                                                    const vector<ColumnBinding> &state) const {
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
		auto correlated_idx = GetCorrelatedIndex(cte_corr_cols[cte_corr_start + i].binding);
		if (!correlated_idx.IsValid()) {
			continue;
		}
		auto j = correlated_idx.GetIndex();
		JoinCondition cond(
		    make_uniq<BoundColumnRefExpression>(correlated_columns[j].type,
		                                        ColumnBinding(cteref.table_index, ProjectionIndex(cte_ref_offset + i))),
		    make_uniq<BoundColumnRefExpression>(correlated_columns[j].type, state[j]),
		    ExpressionType::COMPARE_NOT_DISTINCT_FROM);
		join.conditions.push_back(std::move(cond));
	}
}

void FlattenDependentJoins::AddCorrelatedJoinConditions(LogicalJoin &join, const vector<ColumnBinding> &left_state,
                                                        const vector<ColumnBinding> &right_state) const {
	if (join.type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN ||
	    join.type == LogicalOperatorType::LOGICAL_ASOF_JOIN) {
		AddComparisonJoinConditions(join.Cast<LogicalComparisonJoin>(), left_state, right_state);
		return;
	}
	auto &logical_any_join = join.Cast<LogicalAnyJoin>();
	for (idx_t i = 0; i < correlated_columns.size(); i++) {
		auto left = make_uniq<BoundColumnRefExpression>(correlated_columns[i].type, left_state[i]);
		auto right = make_uniq<BoundColumnRefExpression>(correlated_columns[i].type, right_state[i]);
		auto comparison = make_uniq<BoundComparisonExpression>(ExpressionType::COMPARE_NOT_DISTINCT_FROM,
		                                                       std::move(left), std::move(right));
		auto conjunction = make_uniq<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_AND, std::move(comparison),
		                                                         std::move(logical_any_join.condition));
		logical_any_join.condition = std::move(conjunction);
	}
}

vector<ColumnBinding> FlattenDependentJoins::CreateDelimCrossProduct(unique_ptr<LogicalOperator> &plan,
                                                                     unique_ptr<LogicalOperator> delim_scan,
                                                                     vector<ColumnBinding> state) const {
	auto cross_product = LogicalCrossProduct::Create(std::move(plan), std::move(delim_scan));
	if (cross_product->type != LogicalOperatorType::LOGICAL_CROSS_PRODUCT) {
		auto bindings = cross_product->GetColumnBindings();
		state = vector<ColumnBinding>(
		    bindings.begin(),
		    bindings.begin() + duckdb::NumericCast<vector<ColumnBinding>::difference_type>(correlated_columns.size()));
	}
	plan = std::move(cross_product);
	return std::move(state);
}

vector<ColumnBinding> FlattenDependentJoins::FinalizeDependentJoin(unique_ptr<LogicalOperator> &plan,
                                                                   vector<ColumnBinding> outer_state,
                                                                   const vector<ColumnBinding> &right_state) {
	auto &op = plan->Cast<LogicalDependentJoin>();
	RewriteCorrelatedExpressions::Rewrite(op, correlated_base_bindings, outer_state, correlated_aliases);
	PopulateDuplicateEliminatedColumns(op);

	// We are done using the operator as a DEPENDENT JOIN, it is now fully decorrelated,
	// and we change the type to a DELIM JOIN.
	plan->type = LogicalOperatorType::LOGICAL_DELIM_JOIN;

	auto plan_columns = plan->children[1]->GetColumnBindings();
	CreateDelimJoinConditions(op, op.correlated_columns, right_state, op.perform_delim);

	if (op.is_lateral_join && op.subquery_type == SubqueryType::INVALID) {
		// check if there are any arbitrary expressions left
		if (op.arbitrary_expressions.empty()) {
			return std::move(outer_state);
		}
		// we can only evaluate scalar arbitrary expressions for inner joins
		if (op.join_type != JoinType::INNER) {
			throw BinderException("Join condition for non-inner LATERAL JOIN must be a comparison between the left "
			                      "and right side");
		}
		auto filter = make_uniq<LogicalFilter>();
		filter->expressions = std::move(op.arbitrary_expressions);
		filter->AddChild(std::move(plan));
		plan = std::move(filter);
		return std::move(outer_state);
	}

	if (op.subquery_type == SubqueryType::ANY) {
		AddAnyJoinConditions(op, plan_columns);
	}
	return std::move(outer_state);
}

vector<ColumnBinding> FlattenDependentJoins::PushDownSingleCorrelatedChild(unique_ptr<LogicalOperator> &plan,
                                                                           bool propagate_null_values,
                                                                           vector<ColumnBinding> state,
                                                                           bool correlated_left) {
	idx_t correlated_idx = correlated_left ? 0 : 1;
	idx_t independent_idx = correlated_left ? 1 : 0;
	state = PushDownDependentJoinInternal(plan->children[correlated_idx], propagate_null_values, std::move(state));
	plan->children[independent_idx] = DecorrelateIndependent(binder, std::move(plan->children[independent_idx]));
	return std::move(state);
}

vector<ColumnBinding> FlattenDependentJoins::PushDownDependentJoin(unique_ptr<LogicalOperator> &plan,
                                                                   bool propagate_null_values,
                                                                   vector<ColumnBinding> state) {
	state = PushDownDependentJoinInternal(plan, propagate_null_values, std::move(state));
	if (!replacement_map.empty()) {
		// check if we have to replace any COUNT aggregates into "CASE WHEN X IS NULL THEN 0 ELSE COUNT END"
		RewriteCountAggregates::Rewrite(*plan, replacement_map);
	}
	ColumnBindingResolver::Verify(*plan);
	return std::move(state);
}

void FlattenDependentJoins::AppendDelimColumns(vector<unique_ptr<Expression>> &expressions,
                                               const vector<ColumnBinding> &state, bool include_names) const {
	auto key_count = perform_delim ? correlated_columns.size() : 1;
	for (idx_t i = 0; i < key_count; i++) {
		auto &col = correlated_columns[GetDelimKeyIndex(i)];
		if (include_names) {
			expressions.push_back(make_uniq<BoundColumnRefExpression>(col.name, col.type, state[GetDelimKeyIndex(i)]));
		} else {
			expressions.push_back(make_uniq<BoundColumnRefExpression>(col.type, state[GetDelimKeyIndex(i)]));
		}
	}
}

void FlattenDependentJoins::AppendCorrelatedColumns(vector<unique_ptr<Expression>> &expressions,
                                                    const vector<ColumnBinding> &state, bool include_names) const {
	for (idx_t i = 0; i < correlated_columns.size(); i++) {
		auto &col = correlated_columns[i];
		if (include_names) {
			expressions.push_back(make_uniq<BoundColumnRefExpression>(col.name, col.type, state[i]));
		} else {
			expressions.push_back(make_uniq<BoundColumnRefExpression>(col.type, state[i]));
		}
	}
}

void FlattenDependentJoins::AddDelimColumnsToGroup(LogicalAggregate &aggr, const vector<ColumnBinding> &state) const {
	auto key_count = perform_delim ? correlated_columns.size() : 1;
	for (idx_t i = 0; i < key_count; i++) {
		auto &col = correlated_columns[GetDelimKeyIndex(i)];
		auto colref = make_uniq<BoundColumnRefExpression>(col.name, col.type, state[GetDelimKeyIndex(i)]);
		auto new_group_index = ColumnBinding::PushExpression(aggr.groups, std::move(colref));
		for (auto &set : aggr.grouping_sets) {
			set.insert(new_group_index);
		}
	}
}

void FlattenDependentJoins::AddCorrelatedFirstAggregates(LogicalAggregate &aggr,
                                                         const vector<ColumnBinding> &state) const {
	for (idx_t i = 0; i < correlated_columns.size(); i++) {
		auto &col = correlated_columns[i];
		auto first_aggregate = FirstFunctionGetter::GetFunction(col.type);
		auto colref = make_uniq<BoundColumnRefExpression>(col.name, col.type, state[i]);
		vector<unique_ptr<Expression>> aggr_children;
		aggr_children.push_back(std::move(colref));
		auto first_fun = make_uniq<BoundAggregateExpression>(std::move(first_aggregate), std::move(aggr_children),
		                                                     nullptr, nullptr, AggregateType::NON_DISTINCT);
		aggr.expressions.push_back(std::move(first_fun));
	}
}

vector<ColumnBinding> FlattenDependentJoins::PushDownFilter(unique_ptr<LogicalOperator> &plan,
                                                            bool propagate_null_values, vector<ColumnBinding> state) {
	auto old_child_bindings = plan->children[0]->GetColumnBindings();
	state = PushDownDependentJoinInternal(plan->children[0], propagate_null_values, std::move(state));
	RemapLocalBindings(*plan, old_child_bindings, plan->children[0]->GetColumnBindings());

	RewriteCorrelatedExpressions::Rewrite(*plan, correlated_base_bindings, state, correlated_aliases);
	return std::move(state);
}

vector<ColumnBinding> FlattenDependentJoins::FinalizeProjection(unique_ptr<LogicalOperator> &plan,
                                                                const vector<ColumnBinding> &state,
                                                                const vector<ColumnBinding> &old_child_bindings) {
	RemapLocalBindings(*plan, old_child_bindings, plan->children[0]->GetColumnBindings());

	RewriteCorrelatedExpressions::Rewrite(*plan, correlated_base_bindings, state, correlated_aliases);
	AppendCorrelatedColumns(plan->expressions, state, true);
	auto &proj = plan->Cast<LogicalProjection>();
	auto correlated_offset = plan->expressions.size() - correlated_columns.size();
	ColumnBindingResolver::Verify(*plan);
	return CreateContiguousState(ColumnBinding(proj.table_index, ProjectionIndex(correlated_offset)));
}

vector<ColumnBinding> FlattenDependentJoins::PushDownProjection(unique_ptr<LogicalOperator> &plan,
                                                                bool propagate_null_values,
                                                                vector<ColumnBinding> state) {
	auto old_child_bindings = plan->children[0]->GetColumnBindings();
	for (auto &expr : plan->expressions) {
		propagate_null_values &= expr->PropagatesNullValues();
	}

	propagate_null_values &= plan->children[0]->type != LogicalOperatorType::LOGICAL_DEPENDENT_JOIN;
	state = PushDownDependentJoinInternal(plan->children[0], propagate_null_values, std::move(state));
	return FinalizeProjection(plan, state, old_child_bindings);
}

vector<ColumnBinding> FlattenDependentJoins::PushDownAggregate(unique_ptr<LogicalOperator> &plan,
                                                               bool propagate_null_values,
                                                               vector<ColumnBinding> state) {
	auto &aggr = plan->Cast<LogicalAggregate>();
	auto old_child_bindings = plan->children[0]->GetColumnBindings();
	for (auto &expr : plan->expressions) {
		propagate_null_values &= expr->PropagatesNullValues();
	}
	state = PushDownDependentJoinInternal(plan->children[0], propagate_null_values, std::move(state));
	RemapLocalBindings(*plan, old_child_bindings, plan->children[0]->GetColumnBindings());

	RewriteCorrelatedExpressions::Rewrite(*plan, correlated_base_bindings, state, correlated_aliases);

	TableIndex delim_table_index;
	idx_t delim_column_offset;
	auto new_group_count = perform_delim ? correlated_columns.size() : 1;
	AddDelimColumnsToGroup(aggr, state);
	if (!perform_delim) {
		delim_table_index = aggr.aggregate_index;
		delim_column_offset = aggr.expressions.size();
		AddCorrelatedFirstAggregates(aggr, state);
	} else {
		delim_table_index = aggr.group_index;
		delim_column_offset = aggr.groups.size() - correlated_columns.size();
	}
	bool ungrouped_join = false;
	if (aggr.grouping_sets.empty()) {
		ungrouped_join = aggr.groups.size() == new_group_count;
	} else {
		for (auto &grouping_set : aggr.grouping_sets) {
			ungrouped_join |= grouping_set.size() == new_group_count;
		}
	}
	if (!ungrouped_join) {
		return CreateContiguousState(ColumnBinding(delim_table_index, ProjectionIndex(delim_column_offset)));
	}

	JoinType join_type = JoinType::INNER;
	if (any_join || !propagate_null_values) {
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
		auto &col = correlated_columns[GetDelimKeyIndex(i)];
		JoinCondition cond(
		    make_uniq<BoundColumnRefExpression>(col.name, col.type, ColumnBinding(left_index, ProjectionIndex(i))),
		    make_uniq<BoundColumnRefExpression>(
		        col.type, ColumnBinding(delim_table_index, ProjectionIndex(delim_column_offset + i))),
		    ExpressionType::COMPARE_NOT_DISTINCT_FROM);
		join->conditions.push_back(std::move(cond));
	}
	for (idx_t i = 0; i < aggr.expressions.size(); i++) {
		D_ASSERT(aggr.expressions[i]->GetExpressionClass() == ExpressionClass::BOUND_AGGREGATE);
		auto &bound = aggr.expressions[i]->Cast<BoundAggregateExpression>();
		if (bound.function == CountFunctionBase::GetFunction() || bound.function == CountStarFun::GetFunction()) {
			replacement_map[ColumnBinding(aggr.aggregate_index, ProjectionIndex(i))] = i;
		}
	}
	plan = std::move(join);
	return CreateContiguousState(ColumnBinding(left_index, ProjectionIndex(0)));
}

vector<ColumnBinding> FlattenDependentJoins::PushDownCrossProduct(unique_ptr<LogicalOperator> &plan,
                                                                  bool propagate_null_values,
                                                                  vector<ColumnBinding> state) {
	if (!DependsOnCorrelated(*plan->children[1])) {
		return PushDownSingleCorrelatedChild(plan, propagate_null_values, std::move(state), true);
	}
	if (!DependsOnCorrelated(*plan->children[0])) {
		return PushDownSingleCorrelatedChild(plan, propagate_null_values, std::move(state), false);
	}

	auto join = make_uniq<LogicalComparisonJoin>(JoinType::INNER);
	auto right_state = PushDownDependentJoinInternal(plan->children[1], propagate_null_values, state);
	auto left_state = PushDownDependentJoinInternal(plan->children[0], propagate_null_values, right_state);
	AddComparisonJoinConditions(*join, left_state, right_state);
	join->children.push_back(std::move(plan->children[0]));
	join->children.push_back(std::move(plan->children[1]));
	plan = std::move(join);
	return left_state;
}

vector<ColumnBinding> FlattenDependentJoins::PushDownJoin(unique_ptr<LogicalOperator> &plan, bool propagate_null_values,
                                                          vector<ColumnBinding> state) {
	auto &join = plan->Cast<LogicalJoin>();
	D_ASSERT(plan->children.size() == 2);
	auto old_left_bindings = plan->children[0]->GetColumnBindings();
	auto old_right_bindings = plan->children[1]->GetColumnBindings();
	bool left_has_correlation = DependsOnCorrelated(*plan->children[0]);
	bool right_has_correlation = DependsOnCorrelated(*plan->children[1]);

	bool can_push_left_only =
	    !right_has_correlation && (join.join_type == JoinType::INNER || join.join_type == JoinType::LEFT);
	bool can_push_right_only =
	    !left_has_correlation && (join.join_type == JoinType::INNER || join.join_type == JoinType::RIGHT);

	if (can_push_left_only) {
		return PushDownSingleCorrelatedChild(plan, propagate_null_values, std::move(state), true);
	}
	if (can_push_right_only) {
		return PushDownSingleCorrelatedChild(plan, propagate_null_values, std::move(state), false);
	}

	if (join.join_type == JoinType::MARK) {
		if (!left_has_correlation && right_has_correlation) {
			auto right_state = PushDownDependentJoinInternal(plan->children[1], propagate_null_values, state);
			RemapLocalBindings(*plan, old_right_bindings, plan->children[1]->GetColumnBindings());

			auto left_state = PushDownDependentJoinInternal(plan->children[0], propagate_null_values, right_state);
			RemapLocalBindings(*plan, old_left_bindings, plan->children[0]->GetColumnBindings());

			AddComparisonJoinConditions(join.Cast<LogicalComparisonJoin>(), left_state, right_state);
			return left_state;
		}

		auto left_state = PushDownDependentJoinInternal(plan->children[0], propagate_null_values, state);
		RemapLocalBindings(*plan, old_left_bindings, plan->children[0]->GetColumnBindings());
		plan->children[1] = DecorrelateIndependent(binder, std::move(plan->children[1]));
		RemapLocalBindings(*plan, old_right_bindings, plan->children[1]->GetColumnBindings());
		RewriteCorrelatedExpressions::Rewrite(*plan, correlated_base_bindings, left_state, correlated_aliases);
		return left_state;
	}

	if (join.join_type != JoinType::INNER && join.join_type != JoinType::LEFT && join.join_type != JoinType::RIGHT) {
		throw NotImplementedException("Unsupported join type for flattening correlated subquery");
	}

	auto left_state = PushDownDependentJoinInternal(plan->children[0], propagate_null_values, state);
	RemapLocalBindings(*plan, old_left_bindings, plan->children[0]->GetColumnBindings());

	auto right_state = PushDownDependentJoinInternal(plan->children[1], propagate_null_values, left_state);
	RemapLocalBindings(*plan, old_right_bindings, plan->children[1]->GetColumnBindings());

	AddCorrelatedJoinConditions(join, left_state, right_state);
	RewriteCorrelatedExpressions::Rewrite(*plan, correlated_base_bindings, right_state, correlated_aliases);

	return (join.join_type == JoinType::LEFT) ? left_state : right_state;
}

vector<ColumnBinding> FlattenDependentJoins::PushDownLimit(unique_ptr<LogicalOperator> &plan,
                                                           bool propagate_null_values, vector<ColumnBinding> state) {
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
	unique_ptr<LogicalOrder> order_by = nullptr;
	if (plan->children[0]->type == LogicalOperatorType::LOGICAL_ORDER_BY) {
		order_by = unique_ptr_cast<LogicalOperator, LogicalOrder>(std::move(plan->children[0]));
		plan->children[0] = std::move(order_by->children[0]); // Skip the order_by in the tree temporarily
	}

	state = PushDownDependentJoinInternal(plan->children[0], propagate_null_values, std::move(state));
	auto child = std::move(plan->children[0]);
	auto child_column_count = child->GetColumnBindings().size();

	auto window_index = binder.GenerateTableIndex();
	vector<unique_ptr<Expression>> partitions;
	AppendDelimColumns(partitions, state, true);

	auto window = CreateRowNumberWindow(std::move(child), window_index, std::move(partitions),
	                                    order_by ? std::move(order_by->orders) : vector<BoundOrderByNode>());

	auto filter = make_uniq<LogicalFilter>();
	auto row_num_ref = make_uniq<BoundColumnRefExpression>("limit_rownum", LogicalType::BIGINT,
	                                                       ColumnBinding(window_index, ProjectionIndex(0)));
	unique_ptr<Expression> condition = nullptr;

	if (limit.limit_val.Type() == LimitNodeType::CONSTANT_VALUE) {
		auto limit_val = limit.limit_val.GetConstantValue();
		if (limit.offset_val.Type() == LimitNodeType::CONSTANT_VALUE) {
			TryAddOperator::Operation(limit_val, limit.offset_val.GetConstantValue(), limit_val);
		}
		auto upper_bound = make_uniq<BoundConstantExpression>(Value::BIGINT(limit_val));
		condition = make_uniq<BoundComparisonExpression>(ExpressionType::COMPARE_LESSTHANOREQUALTO, row_num_ref->Copy(),
		                                                 std::move(upper_bound));
	}

	if (limit.offset_val.Type() == LimitNodeType::CONSTANT_VALUE) {
		auto lower_bound = make_uniq<BoundConstantExpression>(Value::BIGINT(limit.offset_val.GetConstantValue()));
		auto lower_comp = make_uniq_base<Expression, BoundComparisonExpression>(
		    ExpressionType::COMPARE_GREATERTHAN, row_num_ref->Copy(), std::move(lower_bound));

		// Stitch together with AND if both bounds exist
		condition = condition ? make_uniq<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_AND,
		                                                              std::move(lower_comp), std::move(condition))
		                      : std::move(lower_comp);
	}

	filter->expressions.push_back(std::move(condition));
	filter->children.push_back(std::move(window));
	for (idx_t i = 0; i < child_column_count; i++) {
		filter->projection_map.emplace_back(i);
	}

	plan = std::move(filter);
	return state;
}

vector<ColumnBinding> FlattenDependentJoins::PushDownWindow(unique_ptr<LogicalOperator> &plan,
                                                            bool propagate_null_values, vector<ColumnBinding> state) {
	auto &window = plan->Cast<LogicalWindow>();
	auto old_child_bindings = plan->children[0]->GetColumnBindings();
	state = PushDownDependentJoinInternal(plan->children[0], propagate_null_values, std::move(state));
	RemapLocalBindings(*plan, old_child_bindings, plan->children[0]->GetColumnBindings());

	RewriteCorrelatedExpressions::Rewrite(*plan, correlated_base_bindings, state, correlated_aliases);

	for (auto &expr : window.expressions) {
		D_ASSERT(expr->GetExpressionClass() == ExpressionClass::BOUND_WINDOW);
		auto &w = expr->Cast<BoundWindowExpression>();
		AppendCorrelatedColumns(w.partitions, state, false);
	}
	return std::move(state);
}

vector<ColumnBinding> FlattenDependentJoins::PushDownSetOperation(unique_ptr<LogicalOperator> &plan,
                                                                  vector<ColumnBinding> state) {
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
		state = PushDownDependentJoin(child, true, std::move(state));
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
	state = CreateContiguousState(ColumnBinding(setop.table_index, ProjectionIndex(setop.column_count)));
	setop.column_count += correlated_columns.size();
	return std::move(state);
}

vector<ColumnBinding> FlattenDependentJoins::PushDownDistinct(unique_ptr<LogicalOperator> &plan,
                                                              vector<ColumnBinding> state) {
	auto &distinct = plan->Cast<LogicalDistinct>();
	auto old_child_bindings = distinct.children[0]->GetColumnBindings();
	state = PushDownDependentJoin(distinct.children[0], true, std::move(state));
	RemapLocalBindings(*plan, old_child_bindings, distinct.children[0]->GetColumnBindings());
	AppendCorrelatedColumns(distinct.distinct_targets, state, false);
	return std::move(state);
}

vector<ColumnBinding> FlattenDependentJoins::PushDownExpressionGet(unique_ptr<LogicalOperator> &plan,
                                                                   bool propagate_null_values,
                                                                   vector<ColumnBinding> state) {
	auto old_child_bindings = plan->children[0]->GetColumnBindings();
	state = PushDownDependentJoinInternal(plan->children[0], propagate_null_values, std::move(state));
	RemapLocalBindings(*plan, old_child_bindings, plan->children[0]->GetColumnBindings());

	RewriteCorrelatedExpressions::Rewrite(*plan, correlated_base_bindings, state, correlated_aliases);

	auto &expr_get = plan->Cast<LogicalExpressionGet>();
	for (auto &expr_list : expr_get.expressions) {
		AppendCorrelatedColumns(expr_list, state, false);
	}
	for (idx_t i = 0; i < correlated_columns.size(); i++) {
		expr_get.expr_types.push_back(correlated_columns[i].type);
	}
	auto correlated_offset = expr_get.expr_types.size() - correlated_columns.size();
	return CreateContiguousState(ColumnBinding(expr_get.table_index, ProjectionIndex(correlated_offset)));
}

vector<ColumnBinding> FlattenDependentJoins::PushDownOrderBy(unique_ptr<LogicalOperator> &plan,
                                                             vector<ColumnBinding> state) {
	auto old_child_bindings = plan->children[0]->GetColumnBindings();
	state = PushDownDependentJoin(plan->children[0], true, std::move(state));
	RemapLocalBindings(*plan, old_child_bindings, plan->children[0]->GetColumnBindings());
	return state;
}

vector<ColumnBinding> FlattenDependentJoins::PushDownGet(unique_ptr<LogicalOperator> &plan,
                                                         vector<ColumnBinding> state) {
	auto &get = plan->Cast<LogicalGet>();
	if (get.children.size() != 1) {
		throw InternalException("Flatten dependent joins - logical get encountered without children");
	}
	state = PushDownDependentJoin(plan->children[0], true, std::move(state));
	// find position of each correlated binding in the child's output
	auto child_bindings = plan->children[0]->GetColumnBindings();
	for (idx_t i = 0; i < correlated_columns.size(); i++) {
		auto it = std::find(child_bindings.begin(), child_bindings.end(), state[i]);
		if (it == child_bindings.end()) {
			throw InternalException("Flatten dependent joins - correlated binding not found in child output");
		}
		get.projected_input.push_back(NumericCast<idx_t>(it - child_bindings.begin()));
	}

	RewriteCorrelatedExpressions::Rewrite(*plan, correlated_base_bindings, state, correlated_aliases);
	// state bindings pass through LogicalGet's projected_input — no update needed
	return state;
}

vector<ColumnBinding> FlattenDependentJoins::PushDownCTE(unique_ptr<LogicalOperator> &plan, bool propagate_null_values,
                                                         vector<ColumnBinding> state) {
#ifdef DEBUG
	plan->children[0]->ResolveOperatorTypes();
	plan->children[1]->ResolveOperatorTypes();
#endif
	if (plan->type == LogicalOperatorType::LOGICAL_MATERIALIZED_CTE && !DependsOnCorrelated(*plan->children[0])) {
		if (DependsOnCorrelated(*plan->children[1])) {
			state = PushDownDependentJoinInternal(plan->children[1], propagate_null_values, state);
			plan->children[0] = DecorrelateIndependent(binder, std::move(plan->children[0]));
		}
		return state;
	}

	state = PushDownDependentJoinInternal(plan->children[0], propagate_null_values, std::move(state));

	auto &cte = plan->Cast<LogicalCTE>();

	cte.correlated_columns = correlated_columns;
	binder.recursive_ctes[cte.table_index] = &cte;
	PatchAccessingOperators(*plan->children[1], cte.table_index, correlated_columns);
	dependency_cache.clear();

	// The CTE outputs a contiguous block of bindings. Lock this state in.
	auto cte_state = CreateContiguousState(ColumnBinding(cte.table_index, ProjectionIndex(cte.column_count)));

	// 4. Recursive CTE Specific Setup
	if (plan->type == LogicalOperatorType::LOGICAL_RECURSIVE_CTE) {
		auto &rec_cte = plan->Cast<LogicalRecursiveCTE>();
		if (!rec_cte.key_targets.empty()) {
			AppendCorrelatedColumns(rec_cte.key_targets, cte_state, false);
		}
		for (auto &col : correlated_columns) {
			rec_cte.internal_types.push_back(col.type);
		}
	}

	auto right_state = PushDownDependentJoinInternal(plan->children[1], false, cte_state);
	RewriteCorrelatedExpressions::Rewrite(*plan, correlated_base_bindings, right_state, correlated_aliases);

#ifdef DEBUG
	plan->children[0]->ResolveOperatorTypes();
	plan->children[1]->ResolveOperatorTypes();
#endif

	cte.column_count += correlated_columns.size();

	return (plan->type == LogicalOperatorType::LOGICAL_RECURSIVE_CTE) ? cte_state : right_state;
}

vector<ColumnBinding> FlattenDependentJoins::PushDownCTERef(unique_ptr<LogicalOperator> &plan) {
	auto &cteref = plan->Cast<LogicalCTERef>();
	if (cteref.correlated_columns < correlated_columns.size()) {
		auto delim_index = binder.GenerateTableIndex();
		auto delim_state = CreateContiguousState(ColumnBinding(delim_index, ProjectionIndex(0)));
		auto delim_scan = make_uniq<LogicalDelimGet>(delim_index, delim_types);
		auto join = make_uniq<LogicalComparisonJoin>(JoinType::INNER);
		AddCTERefJoinConditions(*join, cteref, delim_state);
		if (!join->conditions.empty()) {
			join->children.push_back(std::move(plan));
			join->children.push_back(std::move(delim_scan));
			plan = std::move(join);
			return delim_state;
		}
		return CreateDelimCrossProduct(plan, std::move(delim_scan), std::move(delim_state));
	}
	auto correlated_offset = cteref.chunk_types.size() - cteref.correlated_columns;
	return CreateContiguousState(ColumnBinding(cteref.table_index, ProjectionIndex(correlated_offset)));
}

vector<ColumnBinding> FlattenDependentJoins::PushDownDependentJoinInternal(unique_ptr<LogicalOperator> &plan,
                                                                           bool propagate_null_values,
                                                                           vector<ColumnBinding> state) {
	// first check if the logical operator has correlated expressions
	bool has_correlation = DependsOnCorrelated(*plan);
	if (!has_correlation) {
		// we reached a node without correlated expressions
		// we can eliminate the dependent join now and create a simple cross product
		// now create the duplicate eliminated scan for this node

		// create cross product with Delim Join
		auto delim_index = binder.GenerateTableIndex();
		state = CreateContiguousState(ColumnBinding(delim_index, ProjectionIndex(0)));
		auto delim_scan = make_uniq<LogicalDelimGet>(delim_index, delim_types);
		if (plan->type == LogicalOperatorType::LOGICAL_PROJECTION) {
			auto old_child_bindings = plan->children[0]->GetColumnBindings();
			auto child_propagate_null_values = propagate_null_values;
			for (auto &expr : plan->expressions) {
				child_propagate_null_values &= expr->PropagatesNullValues();
			}
			bool child_is_dependent_join = plan->children[0]->type == LogicalOperatorType::LOGICAL_DEPENDENT_JOIN;
			child_propagate_null_values &= !child_is_dependent_join;
			state = Decorrelate(plan->children[0], child_propagate_null_values, std::move(state));
			state = CreateDelimCrossProduct(plan->children[0], std::move(delim_scan), std::move(state));
			return FinalizeProjection(plan, state, old_child_bindings);
		} else if (plan->type == LogicalOperatorType::LOGICAL_CTE_REF) {
			return PushDownCTERef(plan);
		}
		state = Decorrelate(plan, propagate_null_values, std::move(state));
		return CreateDelimCrossProduct(plan, std::move(delim_scan), std::move(state));
	}
	switch (plan->type) {
	case LogicalOperatorType::LOGICAL_UNNEST:
	case LogicalOperatorType::LOGICAL_FILTER: {
		return PushDownFilter(plan, propagate_null_values, std::move(state));
	}
	case LogicalOperatorType::LOGICAL_PROJECTION: {
		return PushDownProjection(plan, propagate_null_values, std::move(state));
	}
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
		return PushDownAggregate(plan, propagate_null_values, std::move(state));
	}
	case LogicalOperatorType::LOGICAL_CROSS_PRODUCT: {
		return PushDownCrossProduct(plan, propagate_null_values, std::move(state));
	}
	case LogicalOperatorType::LOGICAL_DEPENDENT_JOIN: {
		D_ASSERT(plan->children.size() == 2);
		return Decorrelate(plan, propagate_null_values, std::move(state));
	}
	case LogicalOperatorType::LOGICAL_ANY_JOIN:
	case LogicalOperatorType::LOGICAL_ASOF_JOIN:
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN: {
		return PushDownJoin(plan, propagate_null_values, std::move(state));
	}
	case LogicalOperatorType::LOGICAL_LIMIT: {
		return PushDownLimit(plan, propagate_null_values, std::move(state));
	}
	case LogicalOperatorType::LOGICAL_WINDOW: {
		return PushDownWindow(plan, propagate_null_values, std::move(state));
	}
	case LogicalOperatorType::LOGICAL_EXCEPT:
	case LogicalOperatorType::LOGICAL_INTERSECT:
	case LogicalOperatorType::LOGICAL_UNION: {
		return PushDownSetOperation(plan, std::move(state));
	}
	case LogicalOperatorType::LOGICAL_DISTINCT: {
		return PushDownDistinct(plan, std::move(state));
	}
	case LogicalOperatorType::LOGICAL_EXPRESSION_GET: {
		return PushDownExpressionGet(plan, propagate_null_values, std::move(state));
	}
	case LogicalOperatorType::LOGICAL_PIVOT:
		throw BinderException("PIVOT is not supported in correlated subqueries yet");
	case LogicalOperatorType::LOGICAL_ORDER_BY: {
		return PushDownOrderBy(plan, std::move(state));
	}
	case LogicalOperatorType::LOGICAL_GET: {
		return PushDownGet(plan, std::move(state));
	}
	case LogicalOperatorType::LOGICAL_MATERIALIZED_CTE:
	case LogicalOperatorType::LOGICAL_RECURSIVE_CTE: {
		return PushDownCTE(plan, propagate_null_values, std::move(state));
	}
	case LogicalOperatorType::LOGICAL_CTE_REF: {
		return PushDownCTERef(plan);
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
