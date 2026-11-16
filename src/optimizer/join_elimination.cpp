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
#include "duckdb/optimizer/self_join_aggregate.hpp"
#include "duckdb/planner/column_binding.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include <utility>

namespace duckdb {

static bool AnyVolatile(const LogicalOperator &op) {
	bool is_volatile = false;
	LogicalOperatorVisitor::EnumerateExpressions(op, [&](const unique_ptr<Expression> *expr) {
		if ((*expr)->IsVolatile()) {
			is_volatile = true;
		}
	});
	return is_volatile;
}

void JoinElimination::OptimizeJoinChildren(LogicalOperator &op) {
	left_child = CreateChildren();
	right_child = CreateChildren();
	// the joins below must keep the columns this join's conditions read
	left_child->VisitOperatorExpressions(op);
	right_child->VisitOperatorExpressions(op);
	// repeated input rows only repeat the output rows of these joins, and a SEMI or ANTI join never sees how often a
	// row of its right side occurs
	const bool is_volatile = AnyVolatile(op);
	const bool ignores_duplicates = pipe_info.ignores_duplicates && !is_volatile;
	switch (op.Cast<LogicalComparisonJoin>().join_type) {
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
	OptimizeJoinChild(*left_child, op, 0);
	OptimizeJoinChild(*right_child, op, 1);
}

void JoinElimination::OptimizeJoinChild(JoinElimination &child, LogicalOperator &join, idx_t child_idx) {
	if (join.children[child_idx]->type != LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		child.OptimizeInternal(std::move(join.children[child_idx]));
		return;
	}
	// a join that is the child of a join is analysed, in place, in a copy of the child's pipeline that is dropped
	// afterwards: what holds below the nested join says nothing about its output
	auto nested = child.CreateChildren();
	nested->pipe_info.ignores_duplicates = child.pipe_info.ignores_duplicates;
	nested->pipe_info.join_parent = join;
	nested->pipe_info.join_index = child_idx;
	nested->OptimizeJoinChildren(*join.children[child_idx]);
	nested->TryEliminateJoin();
	child.removed_join = nested->removed_join;
}

void JoinElimination::OptimizeChildren(LogicalOperator &op, optional_ptr<LogicalOperator> parent, idx_t idx) {
	if (op.type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		if (!parent) {
			return;
		}
		D_ASSERT(!pipe_info.join_parent);
		pipe_info.join_parent = parent;
		pipe_info.join_index = idx;
		OptimizeJoinChildren(op);
		return;
	}

	VisitOperatorExpressions(op);

	// whether the operators above return the same result when rows of their input are repeated
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY:
		pipe_info.ignores_duplicates = SelfJoinAggregate::AggregateIgnoresDuplicates(op.Cast<LogicalAggregate>());
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
		OptimizeChildren(*op.children[0], op, idx);
		if (invalidates_distinct) {
			pipe_info.distinct_groups = std::move(distinct_groups_above);
		}
	} else {
		children_root = op;
		for (auto &child : op.children) {
			auto child_optimizer = CreateChildren();
			child_optimizer->OptimizeInternal(std::move(child));
			children.emplace_back(std::move(child_optimizer));
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
		D_ASSERT(op.type != LogicalOperatorType::LOGICAL_COMPARISON_JOIN);
		break;
	}
}

unique_ptr<LogicalOperator> JoinElimination::Optimize(unique_ptr<LogicalOperator> op) {
	OptimizeInternal(std::move(op));
	if (pipe_info.join_parent || !children.empty()) {
		pipe_info.root = TryEliminateJoin();
	}
	return std::move(pipe_info.root);
}

bool JoinElimination::RemovedJoin() const {
	return removed_join;
}

void JoinElimination::OptimizeInternal(unique_ptr<LogicalOperator> op) {
	pipe_info.root = std::move(op);
	OptimizeChildren(*pipe_info.root, nullptr, 0);
}

unique_ptr<LogicalOperator> JoinElimination::TryEliminateJoin() {
	D_ASSERT(pipe_info.root);
	if (!children.empty()) {
		D_ASSERT(!pipe_info.join_parent);
		D_ASSERT(children_root);
		D_ASSERT(children.size() == children_root->children.size());

		for (idx_t idx = 0; idx < children.size(); idx++) {
			children_root->children[idx] = children[idx]->TryEliminateJoin();
			removed_join = removed_join || children[idx]->removed_join;
		}
		return std::move(pipe_info.root);
	}
	if (!pipe_info.join_parent) {
		return std::move(pipe_info.root);
	}
	while (TryEliminateJoinInternal()) {
		removed_join = true;
		// the outer child took the place of the join: when it is a join, it is tried in this pipeline in turn
		auto &replacement = *pipe_info.join_parent->children[pipe_info.join_index];
		if (replacement.type != LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
			break;
		}
		OptimizeJoinChildren(replacement);
	}
	return std::move(pipe_info.root);
}

bool JoinElimination::TryEliminateJoinInternal() {
	auto join_parent = pipe_info.join_parent;

	auto &join_op = pipe_info.join_parent->children[pipe_info.join_index];
	// a child that is a join itself was analysed in place
	if (left_child->pipe_info.root) {
		join_op->children[0] = left_child->TryEliminateJoin();
	}
	if (right_child->pipe_info.root) {
		join_op->children[1] = right_child->TryEliminateJoin();
	}
	removed_join = removed_join || left_child->removed_join || right_child->removed_join;

	auto &join = join_op->Cast<LogicalComparisonJoin>();
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
	// outer rows, whatever the inner side holds (a SINGLE join raises an error on a second match, so it stays)
	if (pipe_info.ignores_duplicates && join.join_type != JoinType::SINGLE) {
		join_parent->children[pipe_info.join_index] = std::move(join_op->children[outer_idx]);
		return true;
	}
	if (inner_child->pipe_info.has_filter) {
		return false;
	}

	if (inner_idx == 1) {
		for (auto &distinct : right_child->pipe_info.distinct_groups) {
			pipe_info.distinct_groups[distinct.first] = distinct.second;
		}
	} else {
		for (auto &distinct : left_child->pipe_info.distinct_groups) {
			pipe_info.distinct_groups[distinct.first] = distinct.second;
		}
	}

	if (pipe_info.distinct_groups.empty()) {
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
		if (is_output_unique && !ContainDistinctGroup(col_bindings)) {
			is_output_unique = false;
		}
	}
	if (!is_output_unique) {
		// 3. join result columns in join condition contains a whole distinct group
		auto outer_bindings = join.children[outer_idx]->GetColumnBindings();
		if (ContainDistinctGroup(outer_bindings)) {
			is_output_unique = true;
		}
	}

	if (is_output_unique) {
		join_parent->children[pipe_info.join_index] = std::move(join_op->children[outer_idx]);
	}
	return is_output_unique;
}

bool JoinElimination::ContainDistinctGroup(vector<ColumnBinding> &column_bindings) {
	if (column_bindings.empty()) {
		// a side whose columns were all pruned, or a join without conditions
		return false;
	}
	auto &column_binding = column_bindings[0];
	auto it = pipe_info.distinct_groups.find(column_binding.table_index);
	if (it == pipe_info.distinct_groups.end()) {
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
