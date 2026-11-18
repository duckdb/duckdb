#include "duckdb/optimizer/filter_statistics.hpp"

#include "duckdb/optimizer/filter_pushdown.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/optimizer/statistics_propagator.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/logical_operator_visitor.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_cteref.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_materialized_cte.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_set_operation.hpp"

namespace duckdb {

FilterStatisticsOptimizer::FilterStatisticsOptimizer(Optimizer &optimizer) : optimizer(optimizer) {
}

static bool HasMultipleBindings(const Expression &expr) {
	unordered_set<TableIndex> bindings;
	ExpressionIterator::VisitExpression<BoundColumnRefExpression>(expr, [&](const BoundColumnRefExpression &ref) {
		if (ref.Depth() == 0) {
			bindings.insert(ref.Binding().table_index);
		}
	});
	return bindings.size() > 1;
}

bool FilterStatisticsOptimizer::HasMultiBindingFilter(const LogicalOperator &op) const {
	if (op.type == LogicalOperatorType::LOGICAL_FILTER) {
		for (auto &expr : op.Cast<LogicalFilter>().expressions) {
			if (HasMultipleBindings(*expr)) {
				return true;
			}
		}
	}
	for (auto &child : op.children) {
		if (HasMultiBindingFilter(*child)) {
			return true;
		}
	}
	return false;
}

bool FilterStatisticsOptimizer::ContainsDelimJoin(const LogicalOperator &op) const {
	if (op.type == LogicalOperatorType::LOGICAL_DELIM_JOIN) {
		return true;
	}
	for (auto &child : op.children) {
		if (ContainsDelimJoin(*child)) {
			return true;
		}
	}
	return false;
}

//! Collects the columns that the filters and join conditions of the plan read
static void GetComparedBindings(LogicalOperator &op, column_binding_set_t &bindings) {
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_FILTER:
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
	case LogicalOperatorType::LOGICAL_ANY_JOIN:
	case LogicalOperatorType::LOGICAL_ASOF_JOIN: {
		auto add_binding = [&](const BoundColumnRefExpression &ref) {
			if (ref.Depth() == 0) {
				bindings.insert(ref.Binding());
			}
		};
		LogicalOperatorVisitor::EnumerateExpressions(op, [&](unique_ptr<Expression> *expr) {
			ExpressionIterator::VisitExpression<BoundColumnRefExpression>(**expr, add_binding);
		});
		break;
	}
	default:
		break;
	}
	for (auto &child : op.children) {
		GetComparedBindings(*child, bindings);
	}
}

//! Adds the columns that the compared columns are computed from, so that a comparison above a projection, an
//! aggregate, a set operation or a reference to a materialized CTE is traced to the scans below it.
//! Columns that any other operator computes (a window, an unnest, a recursive CTE, ...) are not traced: a comparison
//! with such a column is only folded by the statistics propagation that runs after the join order.
static void AddSourceBindings(LogicalOperator &op, column_binding_set_t &bindings,
                              unordered_map<TableIndex, vector<ColumnBinding>> &cte_bindings) {
	auto is_compared = [&](TableIndex table_index, idx_t column_idx) {
		return bindings.find(ColumnBinding(table_index, ProjectionIndex(column_idx))) != bindings.end();
	};
	auto add_sources = [&](const Expression &expr) {
		ExpressionIterator::VisitExpression<BoundColumnRefExpression>(expr, [&](const BoundColumnRefExpression &ref) {
			if (ref.Depth() == 0) {
				bindings.insert(ref.Binding());
			}
		});
	};
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_PROJECTION: {
		auto &projection = op.Cast<LogicalProjection>();
		for (idx_t i = 0; i < projection.expressions.size(); i++) {
			if (is_compared(projection.table_index, i)) {
				add_sources(*projection.expressions[i]);
			}
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
		auto &aggregate = op.Cast<LogicalAggregate>();
		for (idx_t i = 0; i < aggregate.groups.size(); i++) {
			if (is_compared(aggregate.group_index, i)) {
				add_sources(*aggregate.groups[i]);
			}
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_UNION:
	case LogicalOperatorType::LOGICAL_EXCEPT:
	case LogicalOperatorType::LOGICAL_INTERSECT: {
		auto &setop = op.Cast<LogicalSetOperation>();
		for (auto &child : setop.children) {
			auto child_bindings = child->GetColumnBindings();
			for (idx_t i = 0; i < MinValue<idx_t>(setop.column_count, child_bindings.size()); i++) {
				if (is_compared(setop.table_index, i)) {
					bindings.insert(child_bindings[i]);
				}
			}
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_MATERIALIZED_CTE: {
		// the references to the CTE are in the query that uses it: trace that before the definition
		cte_bindings[op.Cast<LogicalMaterializedCTE>().table_index] = op.children[0]->GetColumnBindings();
		AddSourceBindings(*op.children[1], bindings, cte_bindings);
		AddSourceBindings(*op.children[0], bindings, cte_bindings);
		return;
	}
	case LogicalOperatorType::LOGICAL_CTE_REF: {
		auto &cte_ref = op.Cast<LogicalCTERef>();
		auto entry = cte_bindings.find(cte_ref.cte_index);
		if (entry == cte_bindings.end()) {
			break;
		}
		auto &definition_bindings = entry->second;
		for (idx_t i = 0; i < MinValue<idx_t>(cte_ref.chunk_types.size(), definition_bindings.size()); i++) {
			if (is_compared(cte_ref.table_index, i)) {
				bindings.insert(definition_bindings[i]);
			}
		}
		break;
	}
	default:
		break;
	}
	for (auto &child : op.children) {
		AddSourceBindings(*child, bindings, cte_bindings);
	}
}

//! Whether a scan of the plan emits one of the bindings from a column that holds only NULLs
static bool ScansAllNullBinding(ClientContext &context, LogicalOperator &op, const column_binding_set_t &bindings) {
	if (op.type == LogicalOperatorType::LOGICAL_GET) {
		auto &get = op.Cast<LogicalGet>();
		auto &column_ids = get.GetColumnIds();
		for (idx_t i = 0; i < column_ids.size(); i++) {
			if (bindings.find(ColumnBinding(get.table_index, ProjectionIndex(i))) == bindings.end()) {
				continue;
			}
			auto stats = StatisticsPropagator::GetColumnStatistics(context, get, column_ids[i]);
			if (StatisticsPropagator::HasNoValidValue(stats.get())) {
				return true;
			}
		}
	}
	for (auto &child : op.children) {
		if (ScansAllNullBinding(context, *child, bindings)) {
			return true;
		}
	}
	return false;
}

bool FilterStatisticsOptimizer::ComparesAllNullColumn(LogicalOperator &plan) const {
	if (optimizer.OptimizerDisabled(OptimizerType::STATISTICS_PROPAGATION)) {
		return false;
	}
	column_binding_set_t bindings;
	GetComparedBindings(plan, bindings);
	if (bindings.empty()) {
		return false;
	}
	unordered_map<TableIndex, vector<ColumnBinding>> cte_bindings;
	AddSourceBindings(plan, bindings, cte_bindings);
	return ScansAllNullBinding(optimizer.context, plan, bindings);
}

void FilterStatisticsOptimizer::Optimize(unique_ptr<LogicalOperator> &plan) {
	if (ContainsDelimJoin(*plan)) {
		return;
	}
	// a comparison with a column that holds only NULLs never passes: propagating the statistics here removes
	// the filters and joins on it before the join order is chosen
	if (!HasMultiBindingFilter(*plan) && !ComparesAllNullColumn(*plan)) {
		return;
	}

	bool filter_bindings_changed = false;
	optimizer.RunOptimizer(OptimizerType::STATISTICS_PROPAGATION, [&]() {
		StatisticsPropagator propagator(optimizer, plan, StatisticsPropagationMode::FILTER_SIMPLIFICATION);
		propagator.PropagateStatistics(plan);
		filter_bindings_changed = propagator.FilterBindingsChanged();
	});
	if (!filter_bindings_changed) {
		return;
	}

	optimizer.RunOptimizer(OptimizerType::FILTER_PUSHDOWN, [&]() {
		FilterPushdown filter_pushdown(optimizer);
		unordered_set<TableIndex> top_bindings;
		filter_pushdown.CheckMarkToSemi(*plan, top_bindings);
		plan = filter_pushdown.Rewrite(std::move(plan));
	});
}

} // namespace duckdb
