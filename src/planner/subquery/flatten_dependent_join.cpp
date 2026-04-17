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
#include "duckdb/planner/operator/list.hpp"
#include "duckdb/planner/subquery/rewrite_correlated_expressions.hpp"
#include "duckdb/planner/operator/logical_dependent_join.hpp"

namespace duckdb {

static bool AddDependencyBindings(column_binding_set_t &target, const column_binding_set_t &source) {
	bool changed = false;
	for (auto &binding : source) {
		changed |= target.insert(binding).second;
	}
	return changed;
}

static void MergeCorrelatedBindings(column_binding_map_t<idx_t> &target, const CorrelatedColumns &source_columns,
                                    const column_binding_map_t<idx_t> &source) {
	vector<vector<ColumnBinding>> bindings_by_idx(source_columns.size());
	for (auto &entry : source) {
		D_ASSERT(entry.second < source_columns.size());
		bindings_by_idx[entry.second].push_back(entry.first);
	}
	for (idx_t correlated_idx = 0; correlated_idx < source_columns.size(); correlated_idx++) {
		auto &binding_group = bindings_by_idx[correlated_idx];
		if (binding_group.empty()) {
			continue;
		}
		optional_idx target_idx;
		for (auto &binding : binding_group) {
			auto target_entry = target.find(binding);
			if (target_entry == target.end()) {
				continue;
			}
			target_idx = target_entry->second;
			break;
		}
		if (!target_idx.IsValid()) {
			continue;
		}
		for (auto &binding : binding_group) {
			auto result = target.emplace(binding, target_idx.GetIndex());
			D_ASSERT(result.second || result.first->second == target_idx.GetIndex());
		}
	}
}

static void RemapLocalBindings(LogicalOperator &op, const vector<ColumnBinding> &old_bindings,
                               const vector<ColumnBinding> &new_bindings) {
	if (old_bindings == new_bindings) {
		return;
	}
	column_binding_set_t old_binding_set(old_bindings.begin(), old_bindings.end());
	column_binding_set_t new_binding_set(new_bindings.begin(), new_bindings.end());
	vector<ColumnBinding> unmatched_old_bindings;
	vector<ColumnBinding> unmatched_new_bindings;
	for (auto &old_binding : old_bindings) {
		if (!new_binding_set.count(old_binding)) {
			unmatched_old_bindings.push_back(old_binding);
		}
	}
	for (auto &new_binding : new_bindings) {
		if (!old_binding_set.count(new_binding)) {
			unmatched_new_bindings.push_back(new_binding);
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

class DecorrelationStateCollector : public LogicalOperatorVisitor {
public:
	explicit DecorrelationStateCollector(FlattenDependentJoins::DecorrelationState &state, Binder &binder)
	    : state(state), binder(binder) {
	}

	void Collect(LogicalOperator &op) {
		VisitOperator(op);
		auto cte_dependencies = CollectCTEDependencies();
		PropagateCTEDependencies(cte_dependencies);
	}

	void VisitOperator(LogicalOperator &op) override {
		column_binding_set_t dependencies;
		unordered_map<TableIndex, reference_set_t<LogicalOperator>> subtree_accessors;
		VisitOperatorChildren(op);
		for (auto &child : op.children) {
			auto child_entry = state.subtree_dependencies.find(*child);
			if (child_entry != state.subtree_dependencies.end()) {
				AddDependencyBindings(dependencies, child_entry->second);
			}
			auto accessor_entry = state.subtree_accessors.find(*child);
			if (accessor_entry != state.subtree_accessors.end()) {
				for (auto &table_entry : accessor_entry->second) {
					auto &target = subtree_accessors[table_entry.first];
					target.insert(table_entry.second.begin(), table_entry.second.end());
				}
			}
		}

		VisitOperatorExpressions(op);
		AddDependencyBindings(dependencies, local_dependencies);
		local_dependencies.clear();

		if (op.type == LogicalOperatorType::LOGICAL_CTE_REF) {
			auto &cteref = op.Cast<LogicalCTERef>();
			subtree_accessors[cteref.cte_index].insert(op);
			if (cteref.correlated_columns > 0) {
				auto rec_cte = binder.recursive_ctes.find(cteref.cte_index);
				if (rec_cte != binder.recursive_ctes.end()) {
					auto &cte_corr_cols = rec_cte->second->Cast<LogicalCTE>().correlated_columns;
					auto cte_corr_start = cte_corr_cols.size() - cteref.correlated_columns;
					for (idx_t i = cte_corr_start; i < cte_corr_cols.size(); i++) {
						dependencies.insert(cte_corr_cols[i].binding);
					}
				}
			}
		}
		if (op.type == LogicalOperatorType::LOGICAL_MATERIALIZED_CTE ||
		    op.type == LogicalOperatorType::LOGICAL_RECURSIVE_CTE) {
			auto &cte = op.Cast<LogicalCTE>();
			bool found_definition = false;
			for (auto &entry : cte_definition_roots) {
				if (entry.first != cte.table_index) {
					continue;
				}
				entry.second = *op.children[0];
				found_definition = true;
				break;
			}
			if (!found_definition) {
				cte_definition_roots.emplace_back(cte.table_index, *op.children[0]);
			}
		}

		state.subtree_dependencies[op] = std::move(dependencies);
		for (auto &table_entry : subtree_accessors) {
			table_entry.second.insert(op);
		}
		state.subtree_accessors[op] = std::move(subtree_accessors);
		visited_operators.insert(op);
	}

	unique_ptr<Expression> VisitReplace(BoundColumnRefExpression &expr, unique_ptr<Expression> *expr_ptr) override {
		if (expr.depth > 0) {
			local_dependencies.insert(expr.binding);
		}
		return nullptr;
	}

	unique_ptr<Expression> VisitReplace(BoundSubqueryExpression &expr, unique_ptr<Expression> *expr_ptr) override {
		if (!expr.IsCorrelated()) {
			return nullptr;
		}
		for (auto &col : expr.binder->correlated_columns) {
			local_dependencies.insert(col.binding);
		}
		return nullptr;
	}

private:
	unordered_map<TableIndex, column_binding_set_t> CollectCTEDependencies() {
		unordered_map<TableIndex, column_binding_set_t> cte_dependencies;
		for (auto &entry : cte_definition_roots) {
			auto dependency_entry = state.subtree_dependencies.find(entry.second.get());
			if (dependency_entry == state.subtree_dependencies.end()) {
				continue;
			}
			cte_dependencies[entry.first] = dependency_entry->second;
		}

		for (auto &entry : cte_definition_roots) {
			auto accessor_entry = state.subtree_accessors.find(entry.second.get());
			if (accessor_entry == state.subtree_accessors.end()) {
				continue;
			}
			auto &target = cte_dependencies[entry.first];
			for (auto &table_entry : accessor_entry->second) {
				auto cte_dependency_entry = cte_dependencies.find(table_entry.first);
				if (cte_dependency_entry == cte_dependencies.end()) {
					continue;
				}
				AddDependencyBindings(target, cte_dependency_entry->second);
			}
		}
		return cte_dependencies;
	}

	void PropagateCTEDependencies(const unordered_map<TableIndex, column_binding_set_t> &cte_dependencies) {
		for (auto &op_ref : visited_operators) {
			auto dependency_entry = state.subtree_dependencies.find(op_ref.get());
			auto accessor_entry = state.subtree_accessors.find(op_ref.get());
			if (dependency_entry == state.subtree_dependencies.end() ||
			    accessor_entry == state.subtree_accessors.end()) {
				continue;
			}
			for (auto &table_entry : accessor_entry->second) {
				auto cte_dependency_entry = cte_dependencies.find(table_entry.first);
				if (cte_dependency_entry == cte_dependencies.end()) {
					continue;
				}
				AddDependencyBindings(dependency_entry->second, cte_dependency_entry->second);
			}
		}
	}

	FlattenDependentJoins::DecorrelationState &state;
	Binder &binder;
	column_binding_set_t local_dependencies;
	vector<pair<TableIndex, reference<LogicalOperator>>> cte_definition_roots;
	reference_set_t<LogicalOperator> visited_operators;
};

void FlattenDependentJoins::DecorrelationState::Collect(Binder &binder, LogicalOperator &op) {
	DecorrelationStateCollector collector(*this, binder);
	collector.Collect(op);
}

bool FlattenDependentJoins::DecorrelationState::IsCollected(LogicalOperator &op) const {
	return subtree_dependencies.find(op) != subtree_dependencies.end() &&
	       subtree_accessors.find(op) != subtree_accessors.end();
}

bool FlattenDependentJoins::DecorrelationState::DependsOnCorrelated(
    LogicalOperator &op, const column_binding_map_t<idx_t> &correlated_map) const {
	auto entry = subtree_dependencies.find(op);
	if (entry == subtree_dependencies.end()) {
		return false;
	}
	for (auto &binding : entry->second) {
		if (correlated_map.find(binding) != correlated_map.end()) {
			return true;
		}
	}
	return false;
}

column_binding_set_t &FlattenDependentJoins::DecorrelationState::DependenciesFor(LogicalOperator &op) {
	return subtree_dependencies[op];
}

optional_ptr<const reference_set_t<LogicalOperator>>
FlattenDependentJoins::DecorrelationState::AccessorsFor(LogicalOperator &op, TableIndex table_index) const {
	auto subtree_entry = subtree_accessors.find(op);
	if (subtree_entry == subtree_accessors.end()) {
		return nullptr;
	}
	auto accessor_entry = subtree_entry->second.find(table_index);
	if (accessor_entry == subtree_entry->second.end()) {
		return nullptr;
	}
	return accessor_entry->second;
}

FlattenDependentJoins::FlattenDependentJoins(Binder &binder, const CorrelatedColumns &correlated, bool perform_delim,
                                             bool any_join, optional_ptr<FlattenDependentJoins> parent,
                                             bool propagate_null_values)
    : binder(binder), correlated_columns(correlated), perform_delim(perform_delim), any_join(any_join),
      propagate_null_values(propagate_null_values), parent(parent) {
	if (parent) {
		decorrelation_state = parent->decorrelation_state;
	}
	for (idx_t i = 0; i < correlated_columns.size(); i++) {
		auto &col = correlated_columns[i];
		if (parent) {
			auto parent_entry = parent->correlated_map.find(col.binding);
			if (parent_entry != parent->correlated_map.end()) {
				for (auto &entry : parent->correlated_map) {
					if (entry.second != parent_entry->second) {
						continue;
					}
					auto result = correlated_map.emplace(entry.first, i);
					D_ASSERT(result.second || result.first->second == i);
				}
			}
		}
		correlated_map[col.binding] = i;
		delim_types.push_back(col.type);
	}
}

FlattenDependentJoins::DecorrelationState &FlattenDependentJoins::GetDecorrelationState(LogicalOperator &op) {
	if (!decorrelation_state) {
		decorrelation_state = make_shared_ptr<DecorrelationState>();
	}
	if (!decorrelation_state->IsCollected(op)) {
		CollectDecorrelationState(op);
	}
	return *decorrelation_state;
}

void FlattenDependentJoins::CollectDecorrelationState(LogicalOperator &op) {
	D_ASSERT(decorrelation_state);
	decorrelation_state->Collect(binder, op);
}

bool FlattenDependentJoins::DependsOnCorrelated(LogicalOperator &op) const {
	D_ASSERT(decorrelation_state);
	return decorrelation_state->DependsOnCorrelated(op, correlated_map);
}

void FlattenDependentJoins::PatchAccessingOperators(LogicalOperator &subtree_root, TableIndex table_index,
                                                    const CorrelatedColumns &correlated_columns) {
	D_ASSERT(decorrelation_state);
	auto accessors = decorrelation_state->AccessorsFor(subtree_root, table_index);
	if (!accessors) {
		return;
	}
	for (auto &op_ref : *accessors.get()) {
		auto &op = op_ref.get();
		auto &dependencies = decorrelation_state->DependenciesFor(op);
		for (auto &column : correlated_columns) {
			dependencies.insert(column.binding);
		}
		if (op.type == LogicalOperatorType::LOGICAL_CTE_REF) {
			auto &reader = op.Cast<LogicalCTERef>();
			if (reader.cte_index == table_index && reader.correlated_columns == 0) {
				for (auto &column : correlated_columns) {
					reader.chunk_types.push_back(column.type);
					reader.bound_columns.push_back(column.name);
				}
				reader.correlated_columns += correlated_columns.size();
			}
			continue;
		}
		if (op.type != LogicalOperatorType::LOGICAL_DEPENDENT_JOIN) {
			continue;
		}
		auto &join = op.Cast<LogicalDependentJoin>();
		bool has_cte_ref_child = false;
		for (auto &child : join.children) {
			if (decorrelation_state->AccessorsFor(*child, table_index)) {
				has_cte_ref_child = true;
				break;
			}
		}
		if (!has_cte_ref_child) {
			continue;
		}
		for (auto &column : correlated_columns) {
			bool contains_binding = false;
			for (auto &existing : join.correlated_columns) {
				if (existing.binding == column.binding) {
					contains_binding = true;
					break;
				}
			}
			if (!contains_binding) {
				join.correlated_columns.AddColumnToBack(column);
			}
		}
	}
}

void FlattenDependentJoins::CreateDelimJoinConditions(LogicalComparisonJoin &delim_join, vector<ColumnBinding> bindings,
                                                      const CorrelatedLayout &layout, bool perform_delim) {
	auto key_count = layout.GetDelimKeyCount(perform_delim);
	for (idx_t i = 0; i < key_count; i++) {
		auto &col = layout.GetDelimKey(i, perform_delim);
		auto binding_idx = layout.GetDelimOffset(i, perform_delim);
		if (binding_idx >= bindings.size()) {
			throw InternalException("Delim join - binding index out of range");
		}
		JoinCondition cond(make_uniq<BoundColumnRefExpression>(col.name, col.type, col.binding),
		                   make_uniq<BoundColumnRefExpression>(col.name, col.type, bindings[binding_idx]),
		                   ExpressionType::COMPARE_NOT_DISTINCT_FROM);
		delim_join.conditions.push_back(std::move(cond));
	}
}

vector<ColumnBinding> FlattenDependentJoins::GetDependentJoinPlanColumns(LogicalOperator &op) {
	if (op.type == LogicalOperatorType::LOGICAL_MATERIALIZED_CTE) {
		return op.children[1]->GetColumnBindings();
	}
	return op.GetColumnBindings();
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
	return flatten.Decorrelate(std::move(plan)).plan;
}

FlattenDependentJoins::PushDownResult FlattenDependentJoins::Decorrelate(unique_ptr<LogicalOperator> plan,
                                                                         CorrelatedLayout layout) {
	switch (plan->type) {
	case LogicalOperatorType::LOGICAL_DEPENDENT_JOIN: {
		return DecorrelateDependentJoin(std::move(plan), std::move(layout));
	}
	default: {
		for (auto &child : plan->children) {
			auto old_child_bindings = child->GetColumnBindings();
			auto child_result = Decorrelate(std::move(child), layout);
			child = std::move(child_result.plan);
			RemapLocalBindings(*plan, old_child_bindings, child->GetColumnBindings());
			layout = std::move(child_result.layout);
		}
	}
	}

	return PushDownResult(std::move(plan), std::move(layout));
}

FlattenDependentJoins::PushDownResult FlattenDependentJoins::DecorrelateDependentJoin(unique_ptr<LogicalOperator> plan,
                                                                                      CorrelatedLayout layout) {
	auto &delim_join = plan;
	auto &op = plan->Cast<LogicalDependentJoin>();
	GetDecorrelationState(*delim_join->children[1]);
	GetDecorrelationState(*delim_join->children[0]);
	auto left_child_has_correlation = DependsOnCorrelated(*delim_join->children[0]);
	layout = PrepareDependentJoinLeft(op, std::move(layout));

	auto flatten_propagate_null_values = op.propagate_null_values;
	FlattenDependentJoins flatten(binder, op.correlated_columns, op.perform_delim, op.any_join, this,
	                              flatten_propagate_null_values);

	if (delim_join->children[1]->type == LogicalOperatorType::LOGICAL_MATERIALIZED_CTE) {
		auto &cte_ref = delim_join->children[1]->Cast<LogicalMaterializedCTE>();
		// check if the left side of the CTE has correlated expressions
		if (!flatten.DependsOnCorrelated(*cte_ref.children[0])) {
			// the left side of the CTE has no correlated expressions, we can push the DEPENDENT_JOIN down
			auto cte = std::move(delim_join->children[1]);
			delim_join->children[1] = std::move(cte->children[1]);
			if (left_child_has_correlation || op.join_type != JoinType::SINGLE) {
				auto decorrelated = Decorrelate(std::move(delim_join), layout);
				cte->children[1] = std::move(decorrelated.plan);
				return PushDownResult(std::move(cte), std::move(decorrelated.layout));
			}
			auto flatten_result = flatten.PushDownDependentJoin(std::move(delim_join->children[1]));
			MergeCorrelatedBindings(correlated_map, flatten.correlated_columns, flatten.correlated_map);
			delim_join->children[1] = std::move(flatten_result.plan);
			if (!parent) {
				layout = flatten_result.layout;
				layout.ShiftOffsets(delim_join->children[0]->GetColumnBindings().size());
			}
			auto decorrelated =
			    FinalizeDependentJoin(std::move(plan), std::move(layout), std::move(flatten_result.layout));
			cte->children[1] = std::move(decorrelated.plan);
			return PushDownResult(std::move(cte), std::move(decorrelated.layout));
		}
	}

	// now we push the dependent join down
	auto flatten_result = flatten.PushDownDependentJoin(std::move(delim_join->children[1]));
	MergeCorrelatedBindings(correlated_map, flatten.correlated_columns, flatten.correlated_map);
	delim_join->children[1] = std::move(flatten_result.plan);
	if (!parent) {
		layout = flatten_result.layout;
		layout.ShiftOffsets(delim_join->children[0]->GetColumnBindings().size());
	}
	return FinalizeDependentJoin(std::move(plan), std::move(layout), std::move(flatten_result.layout));
}

FlattenDependentJoins::CorrelatedLayout FlattenDependentJoins::PrepareDependentJoinLeft(LogicalDependentJoin &op,
                                                                                        CorrelatedLayout layout) {
	// If we have a parent, we unnest the left side of the DEPENDENT JOIN in the parent's context.
	if (parent) {
		// only push the dependent join to the left side, if there is correlation.
		if (DependsOnCorrelated(op)) {
			auto left_result = PushDownDependentJoin(std::move(op.children[0]), layout);
			op.children[0] = std::move(left_result.plan);
			layout = std::move(left_result.layout);
		} else {
			// There might be unrelated correlation, so we have to traverse the tree
			op.children[0] = DecorrelateIndependent(binder, std::move(op.children[0]));
		}

		RewriteCorrelatedExpressions::Rewrite(op, layout.GetBindings(), correlated_map);
		CollectDecorrelationState(op);
	} else {
		auto old_propagate_null_values = propagate_null_values;
		propagate_null_values = true;
		auto left_result = Decorrelate(std::move(op.children[0]), std::move(layout));
		propagate_null_values = old_propagate_null_values;
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
		auto entry = correlated_map.find(cte_corr_cols[cte_corr_start + i].binding);
		if (entry == correlated_map.end()) {
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
                                                                                   CorrelatedLayout outer_layout,
                                                                                   CorrelatedLayout right_layout) {
	auto &op = plan->Cast<LogicalDependentJoin>();
	RewriteCorrelatedExpressions::Rewrite(*plan, outer_layout.GetBindings(), correlated_map);
	PopulateDuplicateEliminatedColumns(op);

	// We are done using the operator as a DEPENDENT JOIN, it is now fully decorrelated,
	// and we change the type to a DELIM JOIN.
	plan->type = LogicalOperatorType::LOGICAL_DELIM_JOIN;

	auto plan_columns = GetDependentJoinPlanColumns(*plan->children[1]);
	CreateDelimJoinConditions(op, plan_columns, right_layout, op.perform_delim);

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
			return PushDownResult(std::move(filter), std::move(outer_layout));
		}
		return PushDownResult(std::move(plan), std::move(outer_layout));
	}

	if (op.subquery_type == SubqueryType::ANY) {
		AddAnyJoinConditions(op, plan_columns);
	}
	return PushDownResult(std::move(plan), std::move(outer_layout));
}

FlattenDependentJoins::PushDownResult
FlattenDependentJoins::PushDownSingleCorrelatedChild(unique_ptr<LogicalOperator> plan, CorrelatedLayout layout,
                                                     bool correlated_left) {
	auto correlated_idx = correlated_left ? 0 : 1;
	auto independent_idx = correlated_left ? 1 : 0;
	layout = PushDownChild(plan->children[correlated_idx], std::move(layout));
	plan->children[independent_idx] = DecorrelateIndependent(binder, std::move(plan->children[independent_idx]));
	if (!correlated_left) {
		layout.ShiftOffsets(plan->children[0]->GetColumnBindings().size());
	}
	return PushDownResult(std::move(plan), std::move(layout));
}

FlattenDependentJoins::PushDownResult FlattenDependentJoins::PushDownDependentJoin(unique_ptr<LogicalOperator> plan,
                                                                                   CorrelatedLayout layout) {
	auto result = PushDownDependentJoinInternal(std::move(plan), std::move(layout));
	if (!replacement_map.empty()) {
		// check if we have to replace any COUNT aggregates into "CASE WHEN X IS NULL THEN 0 ELSE COUNT END"
		RewriteCountAggregates::Rewrite(*result.plan, replacement_map);
	}
	ColumnBindingResolver::Verify(*result.plan);
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

void FlattenDependentJoins::AppendDelimColumns(vector<unique_ptr<Expression>> &expressions,
                                               const CorrelatedLayout &layout, bool include_names) const {
	auto key_count = layout.GetDelimKeyCount(perform_delim);
	for (idx_t i = 0; i < key_count; i++) {
		auto &col = layout.GetDelimKey(i, perform_delim);
		if (include_names) {
			expressions.push_back(
			    make_uniq<BoundColumnRefExpression>(col.name, col.type, layout.GetDelimBinding(i, perform_delim)));
		} else {
			expressions.push_back(
			    make_uniq<BoundColumnRefExpression>(col.type, layout.GetDelimBinding(i, perform_delim)));
		}
	}
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

void FlattenDependentJoins::AddDelimColumnsToGroup(LogicalAggregate &aggr, const CorrelatedLayout &layout) const {
	auto key_count = layout.GetDelimKeyCount(perform_delim);
	for (idx_t i = 0; i < key_count; i++) {
		auto &col = layout.GetDelimKey(i, perform_delim);
		auto colref = make_uniq<BoundColumnRefExpression>(col.name, col.type, layout.GetDelimBinding(i, perform_delim));
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

FlattenDependentJoins::CorrelatedLayout FlattenDependentJoins::PushDownChild(unique_ptr<LogicalOperator> &child,
                                                                             CorrelatedLayout layout) {
	auto result = PushDownDependentJoinInternal(std::move(child), std::move(layout));
	child = std::move(result.plan);
	return std::move(result.layout);
}

FlattenDependentJoins::CorrelatedLayout FlattenDependentJoins::PushDownFinalizingChild(unique_ptr<LogicalOperator> &child,
                                                                                        CorrelatedLayout layout) {
	auto result = PushDownDependentJoin(std::move(child), std::move(layout));
	child = std::move(result.plan);
	return std::move(result.layout);
}

FlattenDependentJoins::PushDownResult FlattenDependentJoins::PushDownFilter(unique_ptr<LogicalOperator> plan,
                                                                            CorrelatedLayout layout) {
	auto old_child_bindings = plan->children[0]->GetColumnBindings();
	for (auto &expr : plan->expressions) {
		any_join |= SubqueryDependentFilter(*expr);
	}
	layout = PushDownChild(plan->children[0], std::move(layout));
	RemapLocalBindings(*plan, old_child_bindings, plan->children[0]->GetColumnBindings());

	RewriteCorrelatedExpressions::Rewrite(*plan, layout.GetBindings(), correlated_map);
	return PushDownResult(std::move(plan), std::move(layout));
}

FlattenDependentJoins::PushDownResult
FlattenDependentJoins::FinalizeProjection(unique_ptr<LogicalOperator> plan, CorrelatedLayout layout,
                                          const vector<ColumnBinding> &old_child_bindings) {
	RemapLocalBindings(*plan, old_child_bindings, plan->children[0]->GetColumnBindings());

	RewriteCorrelatedExpressions::Rewrite(*plan, layout.GetBindings(), correlated_map);
	AppendCorrelatedColumns(plan->expressions, layout, correlated_columns.size(), true);
	auto &proj = plan->Cast<LogicalProjection>();
	auto correlated_offset = plan->expressions.size() - correlated_columns.size();
	layout = CorrelatedLayout::CreateContiguous(
	    correlated_columns, ColumnBinding(proj.table_index, ProjectionIndex(correlated_offset)), correlated_offset);
	ColumnBindingResolver::Verify(*plan);
	return PushDownResult(std::move(plan), std::move(layout));
}

FlattenDependentJoins::PushDownResult FlattenDependentJoins::PushDownProjection(unique_ptr<LogicalOperator> plan,
                                                                                CorrelatedLayout layout) {
	auto old_child_bindings = plan->children[0]->GetColumnBindings();
	auto child_propagate_null_values = propagate_null_values;
	for (auto &expr : plan->expressions) {
		child_propagate_null_values &= expr->PropagatesNullValues();
	}

	bool child_is_dependent_join = plan->children[0]->type == LogicalOperatorType::LOGICAL_DEPENDENT_JOIN;
	child_propagate_null_values &= !child_is_dependent_join;
	auto old_propagate_null_values = propagate_null_values;
	propagate_null_values = child_propagate_null_values;
	layout = PushDownChild(plan->children[0], std::move(layout));
	propagate_null_values = old_propagate_null_values;
	return FinalizeProjection(std::move(plan), std::move(layout), old_child_bindings);
}

FlattenDependentJoins::PushDownResult FlattenDependentJoins::PushDownAggregate(unique_ptr<LogicalOperator> plan,
                                                                               CorrelatedLayout layout) {
	auto &aggr = plan->Cast<LogicalAggregate>();
	auto old_child_bindings = plan->children[0]->GetColumnBindings();
	auto child_propagate_null_values = propagate_null_values;
	for (auto &expr : plan->expressions) {
		child_propagate_null_values &= expr->PropagatesNullValues();
	}
	auto old_propagate_null_values = propagate_null_values;
	propagate_null_values = child_propagate_null_values;
	layout = PushDownChild(plan->children[0], std::move(layout));
	propagate_null_values = old_propagate_null_values;
	RemapLocalBindings(*plan, old_child_bindings, plan->children[0]->GetColumnBindings());

	RewriteCorrelatedExpressions::Rewrite(*plan, layout.GetBindings(), correlated_map);

	TableIndex delim_table_index;
	idx_t delim_column_offset;
	auto new_group_count = layout.GetDelimKeyCount(perform_delim);
	AddDelimColumnsToGroup(aggr, layout);
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
		if (any_join || !child_propagate_null_values) {
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
			auto &col = layout.GetDelimKey(i, perform_delim);
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
			vector<LogicalType> arguments;
			if (bound.function == CountFunctionBase::GetFunction() || bound.function == CountStarFun::GetFunction()) {
				replacement_map[ColumnBinding(aggr.aggregate_index, ProjectionIndex(i))] = i;
			}
		}
		layout =
		    CorrelatedLayout::CreateContiguous(correlated_columns, ColumnBinding(left_index, ProjectionIndex(0)), 0);
		return PushDownResult(std::move(join), std::move(layout));
	}

	layout = CorrelatedLayout::CreateContiguous(correlated_columns,
	                                            ColumnBinding(delim_table_index, ProjectionIndex(delim_column_offset)),
	                                            delim_column_offset);
	return PushDownResult(std::move(plan), std::move(layout));
}

FlattenDependentJoins::PushDownResult FlattenDependentJoins::PushDownCrossProduct(unique_ptr<LogicalOperator> plan,
                                                                                  CorrelatedLayout layout) {
	bool left_has_correlation = DependsOnCorrelated(*plan->children[0]);
	bool right_has_correlation = DependsOnCorrelated(*plan->children[1]);
	if (!right_has_correlation) {
		return PushDownSingleCorrelatedChild(std::move(plan), std::move(layout), true);
	}
	if (!left_has_correlation) {
		return PushDownSingleCorrelatedChild(std::move(plan), std::move(layout), false);
	}

	auto join = make_uniq<LogicalComparisonJoin>(JoinType::INNER);
	auto right_result = PushDownDependentJoinInternal(std::move(plan->children[1]), layout);
	plan->children[1] = std::move(right_result.plan);
	auto left_result = PushDownDependentJoinInternal(std::move(plan->children[0]), right_result.layout);
	plan->children[0] = std::move(left_result.plan);
	AddComparisonJoinConditions(*join, left_result.layout, right_result.layout);
	join->children.push_back(std::move(plan->children[0]));
	join->children.push_back(std::move(plan->children[1]));
	return PushDownResult(std::move(join), std::move(left_result.layout));
}

FlattenDependentJoins::PushDownResult FlattenDependentJoins::PushDownJoin(unique_ptr<LogicalOperator> plan,
                                                                          CorrelatedLayout layout) {
	auto &join = plan->Cast<LogicalJoin>();
	D_ASSERT(plan->children.size() == 2);
	auto old_left_bindings = plan->children[0]->GetColumnBindings();
	auto old_right_bindings = plan->children[1]->GetColumnBindings();
	bool left_has_correlation = DependsOnCorrelated(*plan->children[0]);
	bool right_has_correlation = DependsOnCorrelated(*plan->children[1]);

	if (join.join_type == JoinType::INNER) {
		if (!right_has_correlation) {
			return PushDownSingleCorrelatedChild(std::move(plan), std::move(layout), true);
		}
		if (!left_has_correlation) {
			return PushDownSingleCorrelatedChild(std::move(plan), std::move(layout), false);
		}
	} else if (join.join_type == JoinType::LEFT) {
		if (!right_has_correlation) {
			return PushDownSingleCorrelatedChild(std::move(plan), std::move(layout), true);
		}
	} else if (join.join_type == JoinType::RIGHT) {
		if (!left_has_correlation) {
			return PushDownSingleCorrelatedChild(std::move(plan), std::move(layout), false);
		}
	} else if (join.join_type == JoinType::MARK) {
		if (!left_has_correlation && right_has_correlation) {
			auto right_result = PushDownDependentJoinInternal(std::move(plan->children[1]), layout);
			plan->children[1] = std::move(right_result.plan);
			RemapLocalBindings(*plan, old_right_bindings, plan->children[1]->GetColumnBindings());

			auto left_result = PushDownDependentJoinInternal(std::move(plan->children[0]), right_result.layout);
			plan->children[0] = std::move(left_result.plan);
			RemapLocalBindings(*plan, old_left_bindings, plan->children[0]->GetColumnBindings());

			AddComparisonJoinConditions(join.Cast<LogicalComparisonJoin>(), left_result.layout, right_result.layout);
			return PushDownResult(std::move(plan), std::move(left_result.layout));
		}

		auto left_result = PushDownDependentJoinInternal(std::move(plan->children[0]), layout);
		plan->children[0] = std::move(left_result.plan);
		RemapLocalBindings(*plan, old_left_bindings, plan->children[0]->GetColumnBindings());
		plan->children[1] = DecorrelateIndependent(binder, std::move(plan->children[1]));
		RemapLocalBindings(*plan, old_right_bindings, plan->children[1]->GetColumnBindings());
		RewriteCorrelatedExpressions::Rewrite(*plan, left_result.layout.GetBindings(), correlated_map);
		return PushDownResult(std::move(plan), std::move(left_result.layout));
	} else {
		throw NotImplementedException("Unsupported join type for flattening correlated subquery");
	}

	auto left_result = PushDownDependentJoinInternal(std::move(plan->children[0]), layout);
	plan->children[0] = std::move(left_result.plan);
	RemapLocalBindings(*plan, old_left_bindings, plan->children[0]->GetColumnBindings());
	auto right_result = PushDownDependentJoinInternal(std::move(plan->children[1]), left_result.layout);
	plan->children[1] = std::move(right_result.plan);
	RemapLocalBindings(*plan, old_right_bindings, plan->children[1]->GetColumnBindings());
	CorrelatedLayout result_layout = right_result.layout;
	if (join.join_type == JoinType::LEFT) {
		result_layout = left_result.layout;
	} else if (join.join_type == JoinType::RIGHT) {
		result_layout.ShiftOffsets(plan->children[0]->GetColumnBindings().size());
	}
	AddCorrelatedJoinConditions(join, left_result.layout, right_result.layout);
	RewriteCorrelatedExpressions::Rewrite(*plan, right_result.layout.GetBindings(), correlated_map);
	return PushDownResult(std::move(plan), std::move(result_layout));
}

FlattenDependentJoins::PushDownResult FlattenDependentJoins::PushDownLimit(unique_ptr<LogicalOperator> plan,
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
		layout = PushDownChild(order_by->children[0], std::move(layout));
		child = std::move(order_by->children[0]);
	} else {
		layout = PushDownChild(plan->children[0], std::move(layout));
		child = std::move(plan->children[0]);
	}
	auto child_column_count = child->GetColumnBindings().size();
	auto window_index = binder.GenerateTableIndex();
	auto window = make_uniq<LogicalWindow>(window_index);
	auto rn = make_uniq<WindowFunction>(RowNumberFun::GetFunction());
	auto row_number = make_uniq<BoundWindowExpression>(LogicalType::BIGINT, nullptr, std::move(rn), nullptr);
	AppendDelimColumns(row_number->partitions, layout, true);
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
                                                                            CorrelatedLayout layout) {
	auto &window = plan->Cast<LogicalWindow>();
	auto old_child_bindings = plan->children[0]->GetColumnBindings();
	layout = PushDownChild(plan->children[0], std::move(layout));
	RemapLocalBindings(*plan, old_child_bindings, plan->children[0]->GetColumnBindings());

	RewriteCorrelatedExpressions::Rewrite(*plan, layout.GetBindings(), correlated_map);

	for (auto &expr : window.expressions) {
		D_ASSERT(expr->GetExpressionClass() == ExpressionClass::BOUND_WINDOW);
		auto &w = expr->Cast<BoundWindowExpression>();
		AppendCorrelatedColumns(w.partitions, layout, correlated_columns.size(), false);
	}
	return PushDownResult(std::move(plan), std::move(layout));
}

FlattenDependentJoins::PushDownResult FlattenDependentJoins::PushDownSetOperation(unique_ptr<LogicalOperator> plan,
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
		auto old_propagate_null_values = propagate_null_values;
		propagate_null_values = true;
		layout = PushDownFinalizingChild(child, std::move(layout));
		propagate_null_values = old_propagate_null_values;
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
	layout = CorrelatedLayout::CreateContiguous(
	    correlated_columns, ColumnBinding(setop.table_index, ProjectionIndex(setop.column_count)), setop.column_count);
	setop.column_count += correlated_columns.size();
	return PushDownResult(std::move(plan), std::move(layout));
}

FlattenDependentJoins::PushDownResult FlattenDependentJoins::PushDownDistinct(unique_ptr<LogicalOperator> plan,
                                                                              CorrelatedLayout layout) {
	auto &distinct = plan->Cast<LogicalDistinct>();
	auto old_child_bindings = distinct.children[0]->GetColumnBindings();
	auto old_propagate_null_values = propagate_null_values;
	propagate_null_values = true;
	layout = PushDownFinalizingChild(distinct.children[0], std::move(layout));
	propagate_null_values = old_propagate_null_values;
	RemapLocalBindings(*plan, old_child_bindings, distinct.children[0]->GetColumnBindings());
	AppendCorrelatedColumns(distinct.distinct_targets, layout, correlated_columns.size(), false);
	return PushDownResult(std::move(plan), std::move(layout));
}

FlattenDependentJoins::PushDownResult FlattenDependentJoins::PushDownExpressionGet(unique_ptr<LogicalOperator> plan,
                                                                                   CorrelatedLayout layout) {
	auto old_child_bindings = plan->children[0]->GetColumnBindings();
	layout = PushDownChild(plan->children[0], std::move(layout));
	RemapLocalBindings(*plan, old_child_bindings, plan->children[0]->GetColumnBindings());

	RewriteCorrelatedExpressions::Rewrite(*plan, layout.GetBindings(), correlated_map);

	auto &expr_get = plan->Cast<LogicalExpressionGet>();
	for (auto &expr_list : expr_get.expressions) {
		AppendCorrelatedColumns(expr_list, layout, correlated_columns.size(), false);
	}
	for (idx_t i = 0; i < correlated_columns.size(); i++) {
		expr_get.expr_types.push_back(correlated_columns[i].type);
	}
	auto correlated_offset = expr_get.expr_types.size() - correlated_columns.size();
	layout = CorrelatedLayout::CreateContiguous(
	    correlated_columns, ColumnBinding(expr_get.table_index, ProjectionIndex(correlated_offset)), correlated_offset);
	return PushDownResult(std::move(plan), std::move(layout));
}

FlattenDependentJoins::PushDownResult FlattenDependentJoins::PushDownOrderBy(unique_ptr<LogicalOperator> plan,
                                                                             CorrelatedLayout layout) {
	auto old_child_bindings = plan->children[0]->GetColumnBindings();
	auto old_propagate_null_values = propagate_null_values;
	propagate_null_values = true;
	layout = PushDownFinalizingChild(plan->children[0], std::move(layout));
	propagate_null_values = old_propagate_null_values;
	RemapLocalBindings(*plan, old_child_bindings, plan->children[0]->GetColumnBindings());
	return PushDownResult(std::move(plan), std::move(layout));
}

FlattenDependentJoins::PushDownResult FlattenDependentJoins::PushDownGet(unique_ptr<LogicalOperator> plan,
                                                                         CorrelatedLayout layout) {
	auto &get = plan->Cast<LogicalGet>();
	if (get.children.size() != 1) {
		throw InternalException("Flatten dependent joins - logical get encountered without children");
	}
	auto old_propagate_null_values = propagate_null_values;
	propagate_null_values = true;
	layout = PushDownFinalizingChild(plan->children[0], std::move(layout));
	propagate_null_values = old_propagate_null_values;
	auto correlated_offset = get.GetColumnBindings().size();
	for (idx_t i = 0; i < correlated_columns.size(); i++) {
		get.projected_input.push_back(layout.GetOffset(i));
	}

	RewriteCorrelatedExpressions::Rewrite(*plan, layout.GetBindings(), correlated_map);
	layout.ResetContiguousOffsets(correlated_offset);
	return PushDownResult(std::move(plan), std::move(layout));
}

FlattenDependentJoins::PushDownResult FlattenDependentJoins::PushDownCTE(unique_ptr<LogicalOperator> plan,
                                                                         CorrelatedLayout layout) {
#ifdef DEBUG
	plan->children[0]->ResolveOperatorTypes();
	plan->children[1]->ResolveOperatorTypes();
#endif
	if (plan->type == LogicalOperatorType::LOGICAL_MATERIALIZED_CTE) {
		bool left_has_correlation = DependsOnCorrelated(*plan->children[0]);
		bool right_has_correlation = DependsOnCorrelated(*plan->children[1]);

		if (!left_has_correlation) {
			if (right_has_correlation) {
				layout = PushDownChild(plan->children[1], std::move(layout));
				plan->children[0] = DecorrelateIndependent(binder, std::move(plan->children[0]));
			}
			return PushDownResult(std::move(plan), std::move(layout));
		}
	}
	layout = PushDownChild(plan->children[0], std::move(layout));

	auto &setop = plan->Cast<LogicalCTE>();
	auto table_index = setop.table_index;
	setop.correlated_columns = correlated_columns;
	binder.recursive_ctes[setop.table_index] = &setop;
	PatchAccessingOperators(*plan->children[1], table_index, correlated_columns);
	CollectDecorrelationState(*plan->children[1]);
	layout = CorrelatedLayout::CreateContiguous(
	    correlated_columns, ColumnBinding(setop.table_index, ProjectionIndex(setop.column_count)), setop.column_count);

	if (plan->type == LogicalOperatorType::LOGICAL_RECURSIVE_CTE) {
		auto &rec_cte = plan->Cast<LogicalRecursiveCTE>();

		if (!rec_cte.key_targets.empty()) {
			AppendCorrelatedColumns(rec_cte.key_targets, layout, correlated_columns.size(), false);
		}
		for (idx_t i = 0; i < correlated_columns.size(); i++) {
			rec_cte.internal_types.push_back(correlated_columns[i].type);
		}
	}

	auto old_propagate_null_values = propagate_null_values;
	propagate_null_values = false;
	layout = PushDownChild(plan->children[1], std::move(layout));
	propagate_null_values = old_propagate_null_values;

	RewriteCorrelatedExpressions::Rewrite(*plan, layout.GetBindings(), correlated_map);

#ifdef DEBUG
	plan->children[0]->ResolveOperatorTypes();
	plan->children[1]->ResolveOperatorTypes();
#endif

	if (plan->type == LogicalOperatorType::LOGICAL_RECURSIVE_CTE) {
		layout = CorrelatedLayout::CreateContiguous(
		    correlated_columns, ColumnBinding(setop.table_index, ProjectionIndex(setop.column_count)),
		    setop.column_count);
	}

	setop.column_count += correlated_columns.size();
	return PushDownResult(std::move(plan), std::move(layout));
}

FlattenDependentJoins::PushDownResult FlattenDependentJoins::PushDownCTERef(unique_ptr<LogicalOperator> plan,
                                                                            CorrelatedLayout layout) {
	auto &cteref = plan->Cast<LogicalCTERef>();
	if (cteref.correlated_columns < correlated_columns.size()) {
		auto delim_index = binder.GenerateTableIndex();
		auto left_columns = plan->GetColumnBindings().size();
		auto delim_layout = CorrelatedLayout::CreateContiguous(
		    correlated_columns, ColumnBinding(delim_index, ProjectionIndex(0)), left_columns);
		auto delim_scan = make_uniq<LogicalDelimGet>(delim_index, delim_types);
		auto join = make_uniq<LogicalComparisonJoin>(JoinType::INNER);
		AddCTERefJoinConditions(*join, cteref, delim_layout);
		if (!join->conditions.empty()) {
			join->children.push_back(std::move(plan));
			join->children.push_back(std::move(delim_scan));
			return PushDownResult(std::move(join), std::move(delim_layout));
		}
		auto cross_product = LogicalCrossProduct::Create(std::move(plan), std::move(delim_scan));
		auto result_layout = delim_layout;
		if (cross_product->type != LogicalOperatorType::LOGICAL_CROSS_PRODUCT) {
			result_layout = CorrelatedLayout::CreateLeading(correlated_columns, cross_product->GetColumnBindings());
		}
		return PushDownResult(std::move(cross_product), std::move(result_layout));
	}
	auto correlated_offset = cteref.chunk_types.size() - cteref.correlated_columns;
	layout = CorrelatedLayout::CreateContiguous(
	    correlated_columns, ColumnBinding(cteref.table_index, ProjectionIndex(correlated_offset)), correlated_offset);
	return PushDownResult(std::move(plan), std::move(layout));
}

FlattenDependentJoins::PushDownResult
FlattenDependentJoins::PushDownDependentJoinInternal(unique_ptr<LogicalOperator> plan, CorrelatedLayout layout) {
	// first check if the logical operator has correlated expressions
	bool has_correlation = DependsOnCorrelated(*plan);
	unique_ptr<LogicalOperator> delim_scan;
	if (!has_correlation) {
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
					PatchAccessingOperators(*plan, op.cte_index, rec_cte_op.correlated_columns);
					CollectDecorrelationState(*plan);
					has_correlation = DependsOnCorrelated(*plan);
				}
			}
		}

		if (!has_correlation) {
			// create cross product with Delim Join
			auto delim_index = binder.GenerateTableIndex();
			auto left_columns = plan->GetColumnBindings().size();
			layout = CorrelatedLayout::CreateContiguous(correlated_columns,
			                                            ColumnBinding(delim_index, ProjectionIndex(0)), left_columns);
			delim_scan = make_uniq<LogicalDelimGet>(delim_index, delim_types);
			if (plan->type == LogicalOperatorType::LOGICAL_PROJECTION) {
				auto old_child_bindings = plan->children[0]->GetColumnBindings();
				auto child_propagate_null_values = propagate_null_values;
				for (auto &expr : plan->expressions) {
					child_propagate_null_values &= expr->PropagatesNullValues();
				}
				bool child_is_dependent_join = plan->children[0]->type == LogicalOperatorType::LOGICAL_DEPENDENT_JOIN;
				child_propagate_null_values &= !child_is_dependent_join;
				auto old_propagate_null_values = propagate_null_values;
				propagate_null_values = child_propagate_null_values;
				auto decorrelated = Decorrelate(std::move(plan->children[0]), layout);
				propagate_null_values = old_propagate_null_values;
				auto cross_product = LogicalCrossProduct::Create(std::move(decorrelated.plan), std::move(delim_scan));
				auto result_layout = std::move(decorrelated.layout);
				if (cross_product->type != LogicalOperatorType::LOGICAL_CROSS_PRODUCT) {
					result_layout =
					    CorrelatedLayout::CreateLeading(correlated_columns, cross_product->GetColumnBindings());
				}
				plan->children[0] = std::move(cross_product);
				return FinalizeProjection(std::move(plan), std::move(result_layout), old_child_bindings);
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

				auto decorrelated = Decorrelate(std::move(plan), layout);
				auto cross_product = LogicalCrossProduct::Create(std::move(decorrelated.plan), std::move(delim_scan));
				auto result_layout = layout;
				if (cross_product->type != LogicalOperatorType::LOGICAL_CROSS_PRODUCT) {
					result_layout =
					    CorrelatedLayout::CreateLeading(correlated_columns, cross_product->GetColumnBindings());
				}
				return PushDownResult(std::move(cross_product), std::move(result_layout));
			} else {
				auto decorrelated = Decorrelate(std::move(plan), layout);
				auto cross_product = LogicalCrossProduct::Create(std::move(decorrelated.plan), std::move(delim_scan));
				auto result_layout = layout;
				if (cross_product->type != LogicalOperatorType::LOGICAL_CROSS_PRODUCT) {
					result_layout =
					    CorrelatedLayout::CreateLeading(correlated_columns, cross_product->GetColumnBindings());
				}
				return PushDownResult(std::move(cross_product), std::move(result_layout));
			}
		}
	}
	switch (plan->type) {
	case LogicalOperatorType::LOGICAL_UNNEST:
	case LogicalOperatorType::LOGICAL_FILTER: {
		return PushDownFilter(std::move(plan), std::move(layout));
	}
	case LogicalOperatorType::LOGICAL_PROJECTION: {
		return PushDownProjection(std::move(plan), std::move(layout));
	}
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
		return PushDownAggregate(std::move(plan), std::move(layout));
	}
	case LogicalOperatorType::LOGICAL_CROSS_PRODUCT: {
		return PushDownCrossProduct(std::move(plan), std::move(layout));
	}
	case LogicalOperatorType::LOGICAL_DEPENDENT_JOIN: {
		D_ASSERT(plan->children.size() == 2);
		return Decorrelate(std::move(plan), std::move(layout));
	}
	case LogicalOperatorType::LOGICAL_ANY_JOIN:
	case LogicalOperatorType::LOGICAL_ASOF_JOIN:
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN: {
		return PushDownJoin(std::move(plan), std::move(layout));
	}
	case LogicalOperatorType::LOGICAL_LIMIT: {
		return PushDownLimit(std::move(plan), std::move(layout));
	}
	case LogicalOperatorType::LOGICAL_WINDOW: {
		return PushDownWindow(std::move(plan), std::move(layout));
	}
	case LogicalOperatorType::LOGICAL_EXCEPT:
	case LogicalOperatorType::LOGICAL_INTERSECT:
	case LogicalOperatorType::LOGICAL_UNION: {
		return PushDownSetOperation(std::move(plan), std::move(layout));
	}
	case LogicalOperatorType::LOGICAL_DISTINCT: {
		return PushDownDistinct(std::move(plan), std::move(layout));
	}
	case LogicalOperatorType::LOGICAL_EXPRESSION_GET: {
		return PushDownExpressionGet(std::move(plan), std::move(layout));
	}
	case LogicalOperatorType::LOGICAL_PIVOT:
		throw BinderException("PIVOT is not supported in correlated subqueries yet");
	case LogicalOperatorType::LOGICAL_ORDER_BY: {
		return PushDownOrderBy(std::move(plan), std::move(layout));
	}
	case LogicalOperatorType::LOGICAL_GET: {
		return PushDownGet(std::move(plan), std::move(layout));
	}
	case LogicalOperatorType::LOGICAL_MATERIALIZED_CTE:
	case LogicalOperatorType::LOGICAL_RECURSIVE_CTE: {
		return PushDownCTE(std::move(plan), std::move(layout));
	}
	case LogicalOperatorType::LOGICAL_CTE_REF: {
		return PushDownCTERef(std::move(plan), std::move(layout));
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
