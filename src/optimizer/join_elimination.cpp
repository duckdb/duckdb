#include "duckdb/optimizer/join_elimination.hpp"
#include "duckdb/common/assert.hpp"
#include "duckdb/common/enums/expression_type.hpp"
#include "duckdb/common/enums/join_type.hpp"
#include "duckdb/common/enums/logical_operator_type.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/planner/column_binding.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include <utility>

namespace duckdb {

//! Whether an expression of `op` itself is volatile (LogicalOperator::HasVolatileExpressions covers a whole subtree)
static bool AnyVolatile(const LogicalOperator &op) {
	bool is_volatile = false;
	LogicalOperatorVisitor::EnumerateExpressions(op, [&](const unique_ptr<Expression> *expr) {
		if ((*expr)->IsVolatile()) {
			is_volatile = true;
		}
	});
	return is_volatile;
}

void JoinElimination::OptimizeChild(LogicalOperator &op, idx_t child_idx, JoinElimination &child_optimizer,
                                    bool replaceable) {
	// `op` may select the columns of its child by position: when a join is removed below, the selection is remapped
	auto projection_map = LogicalOperatorVisitor::GetProjectionMap(op, child_idx);
	const bool has_projection_map = projection_map && !projection_map->empty();
	vector<ColumnBinding> bindings_before;
	if (has_projection_map) {
		bindings_before = op.children[child_idx]->GetColumnBindings();
	}
	const auto removed_before = child_optimizer.removed_joins;
	child_optimizer.OptimizeOperator(op.children[child_idx], replaceable);
	if (has_projection_map && child_optimizer.removed_joins != removed_before) {
		LogicalOperatorVisitor::RemapProjectionMap(*projection_map, bindings_before,
		                                           op.children[child_idx]->GetColumnBindings());
	}
}

void JoinElimination::OptimizeJoin(unique_ptr<LogicalOperator> &op) {
	while (true) {
		auto &join = *op;
		// the pipelines of the two sides inherit the references of the operators above; whatever they find below (table
		// filters, distinct groups) stays theirs
		auto left_child = CreateChildren();
		auto right_child = CreateChildren();
		// the joins below must keep the columns this join's conditions read
		left_child->VisitOperatorExpressions(join);
		right_child->VisitOperatorExpressions(join);
		// repeated input rows only repeat the output rows of these joins, and a SEMI or ANTI join never sees how often
		// a row of its right side occurs
		const bool is_volatile = AnyVolatile(join);
		const bool ignores_duplicates = pipe_info.ignores_duplicates && !is_volatile;
		switch (join.Cast<LogicalComparisonJoin>().join_type) {
		case JoinType::INNER:
		case JoinType::LEFT:
		case JoinType::RIGHT:
		case JoinType::OUTER:
			left_child->pipe_info.ignores_duplicates = ignores_duplicates;
			right_child->pipe_info.ignores_duplicates = ignores_duplicates;
			break;
		case JoinType::SEMI:
		case JoinType::ANTI:
			left_child->pipe_info.ignores_duplicates = ignores_duplicates;
			right_child->pipe_info.ignores_duplicates = !is_volatile;
			break;
		default:
			break;
		}
		OptimizeJoinChild(join, 0, *left_child);
		OptimizeJoinChild(join, 1, *right_child);
		removed_joins += left_child->removed_joins + right_child->removed_joins;
		if (!TryEliminateJoin(op, *left_child, *right_child)) {
			return;
		}
		removed_joins++;
		// the outer side took the place of the join: when it is a join, it is tried in this pipeline in turn
		if (op->type != LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
			return;
		}
	}
}

void JoinElimination::OptimizeJoinChild(LogicalOperator &join, idx_t child_idx, JoinElimination &child) {
	if (join.children[child_idx]->type != LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		OptimizeChild(join, child_idx, child, true);
		return;
	}
	// a join that is the child of a join is a pipeline of its own that inherits the references and the
	// duplicate-insensitivity of this side: what is found below the nested join says nothing about its output
	auto nested = child.CreateChildren();
	nested->pipe_info.ignores_duplicates = child.pipe_info.ignores_duplicates;
	OptimizeChild(join, child_idx, *nested, true);
	child.removed_joins += nested->removed_joins;
}

void JoinElimination::OptimizeOperator(unique_ptr<LogicalOperator> &op_ptr, bool replaceable) {
	if (op_ptr->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		if (replaceable) {
			OptimizeJoin(op_ptr);
		}
		return;
	}
	// only the children of `op` are replaced from here on
	auto &op = *op_ptr;

	VisitOperatorExpressions(op);

	// whether the operators above return the same result when rows of their input are repeated
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY:
		pipe_info.ignores_duplicates = op.Cast<LogicalAggregate>().IgnoresDuplicates();
		break;
	case LogicalOperatorType::LOGICAL_PROJECTION:
	case LogicalOperatorType::LOGICAL_FILTER:
		pipe_info.ignores_duplicates = pipe_info.ignores_duplicates && !AnyVolatile(op);
		break;
	default:
		pipe_info.ignores_duplicates = false;
		break;
	}

	switch (op.type) {
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
		auto &aggr = op.Cast<LogicalAggregate>();
		if (aggr.grouping_sets.size() > 1) {
			break;
		}
		// only resolve group by columns for now
		column_binding_set_t distinct_group;
		TableIndex table_idx = aggr.group_index;
		for (idx_t i = 0; i < aggr.groups.size(); i++) {
			distinct_group.insert(ColumnBinding(aggr.group_index, ProjectionIndex(i)));
		}
		if (!distinct_group.empty()) {
			pipe_info.distinct_groups[table_idx] = std::move(distinct_group);
		}
		if (aggr.expressions.empty() && !aggr.groups.empty()) {
			// a pure deduplication (e.g. SELECT DISTINCT) also makes the grouped input columns distinct
			column_binding_set_t input_group;
			TableIndex input_index;
			bool single_input = true;
			for (auto &group : aggr.groups) {
				if (group->GetExpressionType() != ExpressionType::BOUND_COLUMN_REF) {
					single_input = false;
					break;
				}
				auto &binding = group->Cast<BoundColumnRefExpression>().Binding();
				if (!input_group.empty() && binding.table_index != input_index) {
					single_input = false;
					break;
				}
				input_index = binding.table_index;
				input_group.insert(binding);
			}
			if (single_input) {
				pipe_info.distinct_groups[input_index] = std::move(input_group);
			}
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_PROJECTION: {
		auto &projection = op.Cast<LogicalProjection>();
		unordered_map<idx_t, vector<idx_t>> reference_records;
		// for select distinct * from table, first projection then distinct. distinct_groups has record projection table
		// id for select * from table group by col, first aggregate then projection. projection has aggregate table id.

		// before traverse children, first check whether any distinct group ref this projection
		auto it = pipe_info.distinct_groups.find(projection.table_index);
		if (it != pipe_info.distinct_groups.end()) {
			column_binding_set_t new_distinct_group;
			auto &start_expr = projection.GetExpression(*it->second.begin());
			if (start_expr.GetExpressionType() != ExpressionType::BOUND_COLUMN_REF) {
				// if the expression is not a column ref, we cannot eliminate the join
				break;
			}
			bool could_add = true;
			auto ref_id = start_expr.Cast<BoundColumnRefExpression>().Binding().table_index;
			for (auto &col : it->second) {
				auto &expression = projection.GetExpression(col);
				if (expression.GetExpressionType() != ExpressionType::BOUND_COLUMN_REF) {
					// if the expression is not a column ref, we cannot eliminate the join
					could_add = false;
					break;
				}
				auto &col_ref = expression.Cast<BoundColumnRefExpression>();
				if (ref_id != col_ref.Binding().table_index) {
					could_add = false;
					break;
				}
				new_distinct_group.insert(col_ref.Binding());
			}
			if (could_add) {
				pipe_info.distinct_groups[ref_id] = std::move(new_distinct_group);
			}
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_LIMIT:
	case LogicalOperatorType::LOGICAL_TOP_N:
	case LogicalOperatorType::LOGICAL_SAMPLE:
	case LogicalOperatorType::LOGICAL_WINDOW: {
		// these pick rows by cardinality, or compute values that depend on row multiplicity (window), so a
		// distinct group above them does not hold below them
		pipe_info.distinct_groups.clear();
		break;
	}
	case LogicalOperatorType::LOGICAL_GET: {
		auto &get = op.Cast<LogicalGet>();
		if (get.table_filters.HasFilters()) {
			pipe_info.has_filter = true;
		}
		break;
	}
	default:
		break;
	}

	if (op.children.size() == 1) {
		// UNNEST emits multiple rows per input row, so a distinct group found below it does not hold above it
		const auto invalidates_distinct = op.type == LogicalOperatorType::LOGICAL_UNNEST;
		unordered_map<TableIndex, column_binding_set_t> distinct_groups_above;
		if (invalidates_distinct) {
			distinct_groups_above = pipe_info.distinct_groups;
		}
		OptimizeChild(op, 0, *this, true);
		if (invalidates_distinct) {
			pipe_info.distinct_groups = std::move(distinct_groups_above);
		}
	} else {
		// every child starts a pipeline of its own; a child that is a join stays, its columns may be read by position
		for (idx_t child_idx = 0; child_idx < op.children.size(); child_idx++) {
			auto child_optimizer = CreateChildren();
			OptimizeChild(op, child_idx, *child_optimizer, false);
			removed_joins += child_optimizer->removed_joins;
		}
		return;
	}

	switch (op.type) {
	case LogicalOperatorType::LOGICAL_PROJECTION: {
		auto &projection = op.Cast<LogicalProjection>();
		// after traversed children, here check whether any distinct group added in children
		unordered_map<TableIndex, DistinctGroupRef> ref_table_columns;
		for (idx_t proj_idx = 0; proj_idx < projection.expressions.size(); proj_idx++) {
			auto &expression = projection.expressions.get(proj_idx);
			if (expression->GetExpressionType() == ExpressionType::BOUND_COLUMN_REF) {
				auto &col_ref = expression->Cast<BoundColumnRefExpression>();
				auto distinct_group_it = pipe_info.distinct_groups.find(col_ref.Binding().table_index);
				if (distinct_group_it == pipe_info.distinct_groups.end()) {
					continue;
				}
				if (ref_table_columns.find(col_ref.Binding().table_index) == ref_table_columns.end()) {
					auto ref = DistinctGroupRef();
					for (auto &col : distinct_group_it->second) {
						ref.ref_column_ids.insert(col.column_index);
					}
					ref_table_columns[col_ref.Binding().table_index] = ref;
				}
				ref_table_columns[col_ref.Binding().table_index].distinct_group.insert(
				    ColumnBinding(projection.table_index, ProjectionIndex(proj_idx)));
				ref_table_columns[col_ref.Binding().table_index].ref_column_ids.erase(col_ref.Binding().column_index);
			}
		}
		for (auto &refs : ref_table_columns) {
			if (refs.second.ref_column_ids.empty()) {
				pipe_info.distinct_groups[projection.table_index] = std::move(refs.second.distinct_group);
			}
		}
		break;
	}
	default:
		break;
	}
}

unique_ptr<LogicalOperator> JoinElimination::Optimize(unique_ptr<LogicalOperator> op) {
	OptimizeOperator(op, false);
	return op;
}

bool JoinElimination::RemovedJoin() const {
	return removed_joins > 0;
}

bool JoinElimination::TryEliminateJoin(unique_ptr<LogicalOperator> &op, JoinElimination &left_child,
                                       JoinElimination &right_child) {
	auto &join = op->Cast<LogicalComparisonJoin>();
	bool is_output_unique = false;
	idx_t inner_idx = 1;
	idx_t outer_idx = 0;
	switch (join.join_type) {
	case JoinType::LEFT:
		break;
	case JoinType::SINGLE: {
		is_output_unique = true;
		break;
	case JoinType::RIGHT:
		inner_idx = 0;
		outer_idx = 1;
		break;
	}
	default:
		return false;
	}
	auto &inner_child = inner_idx == 0 ? left_child : right_child;
	auto &outer_projection_map = outer_idx == 0 ? join.left_projection_map : join.right_projection_map;
	if (join.filter_pushdown) {
		return false;
	}
	auto inner_bindings = join.children[inner_idx]->GetColumnBindings();
	// ensure join output columns only contains outer table columns
	for (auto &binding : inner_bindings) {
		if (pipe_info.ref_table_ids.find(binding.table_index) != pipe_info.ref_table_ids.end()) {
			return false;
		}
	}
	// 0. the operators above ignore duplicates: an outer join that nothing reads the inner side of only repeats its
	// outer rows, whatever the inner side holds (a SINGLE join raises an error on a second match, so it stays).
	// An inner side or a join condition with a volatile expression (nextval, random) stays as well: it has an effect
	// of its own
	if (pipe_info.ignores_duplicates && join.join_type != JoinType::SINGLE && !AnyVolatile(join) &&
	    !join.children[inner_idx]->HasVolatileExpressions()) {
		op = LogicalJoin::PromoteJoinSide(std::move(join.children[outer_idx]), std::move(outer_projection_map));
		return true;
	}
	if (inner_child.pipe_info.has_filter) {
		return false;
	}

	// the distinct groups that hold for this join: the ones of the operators above it and the ones found on its inner
	// side. They are not kept in this pipeline: they say nothing about the operator that replaces the join
	auto distinct_groups = pipe_info.distinct_groups;
	for (auto &distinct : inner_child.pipe_info.distinct_groups) {
		distinct_groups[distinct.first] = distinct.second;
	}
	if (distinct_groups.empty()) {
		return false;
	}
	// 1. TODO: guarantee by primary/foreign key

	if (!is_output_unique) {
		is_output_unique = true;
		// 2. inner table join condition columns contains a whole distinct group
		vector<ColumnBinding> col_bindings;
		for (auto &condition : join.conditions) {
			if (!condition.IsComparison() || condition.GetComparisonType() != ExpressionType::COMPARE_EQUAL ||
			    condition.GetLHS().GetExpressionType() != ExpressionType::BOUND_COLUMN_REF ||
			    condition.GetRHS().GetExpressionType() != ExpressionType::BOUND_COLUMN_REF) {
				is_output_unique = false;
				break;
			}
			auto inner_binding = inner_idx == 0 ? condition.GetLHS().Cast<BoundColumnRefExpression>().Binding()
			                                    : condition.GetRHS().Cast<BoundColumnRefExpression>().Binding();
			col_bindings.push_back(inner_binding);
		}
		if (is_output_unique && !ContainDistinctGroup(distinct_groups, col_bindings)) {
			is_output_unique = false;
		}
	}
	if (!is_output_unique) {
		// 3. join result columns in join condition contains a whole distinct group
		auto outer_bindings = join.children[outer_idx]->GetColumnBindings();
		if (ContainDistinctGroup(distinct_groups, outer_bindings)) {
			is_output_unique = true;
		}
	}

	if (is_output_unique) {
		op = LogicalJoin::PromoteJoinSide(std::move(join.children[outer_idx]), std::move(outer_projection_map));
	}
	return is_output_unique;
}

bool JoinElimination::ContainDistinctGroup(const unordered_map<TableIndex, column_binding_set_t> &distinct_groups,
                                           vector<ColumnBinding> &column_bindings) {
	if (column_bindings.empty()) {
		// a side whose columns were all pruned, or a join without conditions
		return false;
	}
	auto &column_binding = column_bindings[0];
	auto it = distinct_groups.find(column_binding.table_index);
	if (it == distinct_groups.end()) {
		return false;
	}
	unordered_set<ProjectionIndex> used_column_ids;
	for (auto &binding : column_bindings) {
		if (it->second.find(binding) == it->second.end()) {
			continue;
		}
		used_column_ids.emplace(binding.column_index);
	}
	return used_column_ids.size() == it->second.size();
}

unique_ptr<Expression> JoinElimination::VisitReplace(BoundColumnRefExpression &expr, unique_ptr<Expression> *expr_ptr) {
	pipe_info.ref_table_ids.insert(expr.Binding().table_index);
	return nullptr;
}

} // namespace duckdb
