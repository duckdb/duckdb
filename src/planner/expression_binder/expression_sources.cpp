#include "duckdb/planner/expression_sources.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/query_node/bound_select_node.hpp"

namespace duckdb {

static BoundExpressionSource GetSource(const vector<unique_ptr<Expression>> &expressions, ProjectionIndex index,
                                       ExpressionSourceKind kind) {
	if (index.GetIndex() >= expressions.size() || !expressions[index]) {
		throw InternalException("Registered expression source is unavailable during binding");
	}
	return {expressions[index].get(), kind};
}

BoundExpressionSource BoundSelectNode::GetExpressionSource(ColumnBinding binding) const {
	if (binding.table_index == group_index) {
		return GetSource(groups.group_expressions, binding.column_index, ExpressionSourceKind::GROUP);
	}
	if (binding.table_index == aggregate_index) {
		// Collated groups expose their original values through first() aggregates.
		for (auto &group : bind_state.collated_groups) {
			if (group.second == binding.column_index) {
				return GetSource(aggregates, binding.column_index, ExpressionSourceKind::GROUP);
			}
		}
		return GetSource(aggregates, binding.column_index, ExpressionSourceKind::AGGREGATE);
	}
	if (binding.table_index == window_index) {
		return GetSource(windows, binding.column_index, ExpressionSourceKind::WINDOW);
	}
	for (auto &collection :
	     {reference<const BoundUnnestMap>(unnests.SelectList()), reference<const BoundUnnestMap>(unnests.GroupBy())}) {
		for (auto &entry : collection.get()) {
			if (binding.table_index == entry.second.index) {
				return GetSource(entry.second.expressions, binding.column_index, ExpressionSourceKind::UNNEST);
			}
		}
	}
	return {};
}

BoundExpressionSource Binder::GetExpressionSource(ColumnBinding binding) const {
	for (auto scope = global_binder_state->expression_sources.rbegin();
	     scope != global_binder_state->expression_sources.rend(); ++scope) {
		auto source = scope->get().GetExpressionSource(binding);
		if (source.expression) {
			return source;
		}
	}
	return {};
}

ExpressionSourceScope::ExpressionSourceScope(Binder &binder_p, const BoundSelectNode &node_p)
    : binder(binder_p), node(node_p) {
	D_ASSERT(node->group_index.IsValid() && node->aggregate_index.IsValid() && node->window_index.IsValid());
	binder.global_binder_state->expression_sources.emplace_back(node_p);
}

ExpressionSourceScope::~ExpressionSourceScope() {
	Release();
}

void ExpressionSourceScope::Release() {
	if (!node) {
		return;
	}
	auto &scopes = binder.global_binder_state->expression_sources;
	D_ASSERT(!scopes.empty() && &scopes.back().get() == node.get());
	scopes.pop_back();
	node = nullptr;
}
} // namespace duckdb
