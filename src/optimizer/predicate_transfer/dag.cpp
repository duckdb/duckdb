#include "duckdb/optimizer/predicate_transfer/dag.hpp"

namespace duckdb {

bool FilterPlan::operator==(const FilterPlan &other) const {
	// Compare build expressions
	if (build.size() != other.build.size()) {
		return false;
	}
	for (size_t i = 0; i < build.size(); i++) {
		const auto &lhs_binding = build[i]->Cast<BoundColumnRefExpression>().binding;
		const auto &rhs_binding = other.build[i]->Cast<BoundColumnRefExpression>().binding;
		if (lhs_binding.table_index != rhs_binding.table_index ||
		    lhs_binding.column_index != rhs_binding.column_index) {
			return false;
		}
	}

	// Compare apply expressions
	if (apply.size() != other.apply.size()) {
		return false;
	}
	for (size_t i = 0; i < apply.size(); i++) {
		const auto &lhs_binding = apply[i]->Cast<BoundColumnRefExpression>().binding;
		const auto &rhs_binding = other.apply[i]->Cast<BoundColumnRefExpression>().binding;
		if (lhs_binding.table_index != rhs_binding.table_index ||
		    lhs_binding.column_index != rhs_binding.column_index) {
			return false;
		}
	}

	return true;
}

GraphEdge *GraphNode::Add(idx_t other, bool is_forward, bool is_in_edge) {
	auto &stage = (is_forward ? forward_stage_edges : backward_stage_edges);
	auto &edges = (is_in_edge ? stage.in : stage.out);
	for (auto &edge : edges) {
		if (edge->destination == other) {
			return edge.get();
		}
	}
	edges.emplace_back(make_uniq<GraphEdge>(other));
	return edges.back().get();
}

GraphEdge *GraphNode::Add(idx_t other, Expression *expression, bool is_forward, bool is_in_edge) {
	auto *edge = Add(other, is_forward, is_in_edge);
	edge->conditions.push_back(expression);
	return edge;
}

GraphEdge *GraphNode::Add(idx_t other, const shared_ptr<FilterPlan> &filter_plan, bool is_forward, bool is_in_edge) {
	auto *edge = Add(other, is_forward, is_in_edge);
	edge->filter_plan.push_back(filter_plan);
	return edge;
}

} // namespace duckdb
