#include "duckdb/planner/operator/logical_join.hpp"

#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"

namespace duckdb {

LogicalJoin::LogicalJoin(JoinType join_type, LogicalOperatorType logical_type)
    : LogicalOperator(logical_type), join_type(join_type) {
}

vector<ColumnBinding> LogicalJoin::GetColumnBindings() {
	auto left_bindings = MapBindings(children[0]->GetColumnBindings(), left_projection_map);
	if (join_type == JoinType::SEMI || join_type == JoinType::ANTI) {
		// for SEMI and ANTI join we only project the left hand side
		return left_bindings;
	}

	if (join_type == JoinType::MARK) {
		// for MARK join we project the left hand side plus the MARK column
		left_bindings.emplace_back(mark_index, 0);
		return left_bindings;
	}
	// for other join types we project both the LHS and the RHS
	auto right_bindings = MapBindings(children[1]->GetColumnBindings(), right_projection_map);
	if (join_type == JoinType::RIGHT_SEMI || join_type == JoinType::RIGHT_ANTI) {
		return right_bindings;
	}
	left_bindings.insert(left_bindings.end(), right_bindings.begin(), right_bindings.end());
	return left_bindings;
}

void LogicalJoin::ResolveTypes() {
	types = MapTypes(children[0]->types, left_projection_map);
	if (join_type == JoinType::SEMI || join_type == JoinType::ANTI) {
		// for SEMI and ANTI join we only project the left hand side
		return;
	}
	if (join_type == JoinType::MARK) {
		// for MARK join we project the left hand side, plus a BOOLEAN column indicating the MARK
		types.emplace_back(LogicalType::BOOLEAN);
		return;
	}
	// for any other join we project both sides
	auto right_types = MapTypes(children[1]->types, right_projection_map);
	if (join_type == JoinType::RIGHT_SEMI || join_type == JoinType::RIGHT_ANTI) {
		types = right_types;
		return;
	}
	types.insert(types.end(), right_types.begin(), right_types.end());
}

void LogicalJoin::GetTableReferences(LogicalOperator &op, unordered_set<idx_t> &bindings) {
	auto column_bindings = op.GetColumnBindings();
	for (auto binding : column_bindings) {
		bindings.insert(binding.table_index);
	}
}

void LogicalJoin::GetExpressionBindings(Expression &expr, unordered_set<idx_t> &bindings) {
	if (expr.type == ExpressionType::BOUND_COLUMN_REF) {
		auto &colref = expr.Cast<BoundColumnRefExpression>();
		D_ASSERT(colref.depth == 0);
		bindings.insert(colref.binding.table_index);
	}
	ExpressionIterator::EnumerateChildren(expr, [&](Expression &child) { GetExpressionBindings(child, bindings); });
}

} // namespace duckdb
