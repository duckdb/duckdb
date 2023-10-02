#include "duckdb/optimizer/common_aggregate_optimizer.hpp"

#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/parser/expression_map.hpp"
#include "duckdb/planner/column_binding_map.hpp"

namespace duckdb {

void CommonAggregateOptimizer::VisitOperator(LogicalOperator &op) {
	LogicalOperatorVisitor::VisitOperator(op);
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY:
		ExtractCommonAggregates(op.Cast<LogicalAggregate>());
		break;
	default:
		break;
	}
}

unique_ptr<Expression> CommonAggregateOptimizer::VisitReplace(BoundColumnRefExpression &expr,
                                                              unique_ptr<Expression> *expr_ptr) {
	// check if this column ref points to an aggregate that was remapped; if it does we remap it
	auto entry = aggregate_map.find(expr.binding);
	if (entry != aggregate_map.end()) {
		expr.binding = entry->second;
	}
	return nullptr;
}

void CommonAggregateOptimizer::ExtractCommonAggregates(LogicalAggregate &aggr) {
	expression_map_t<idx_t> aggregate_remap;
	idx_t total_erased = 0;
	for (idx_t i = 0; i < aggr.expressions.size(); i++) {
		idx_t original_index = i + total_erased;
		auto entry = aggregate_remap.find(*aggr.expressions[i]);
		if (entry == aggregate_remap.end()) {
			// aggregate does not exist yet: add it to the map
			aggregate_remap[*aggr.expressions[i]] = i;
			if (i != original_index) {
				// this aggregate is not erased, however an agregate BEFORE it has been erased
				// so we need to remap this aggregaet
				ColumnBinding original_binding(aggr.aggregate_index, original_index);
				ColumnBinding new_binding(aggr.aggregate_index, i);
				aggregate_map[original_binding] = new_binding;
			}
		} else {
			// aggregate already exists! we can remove this entry
			total_erased++;
			aggr.expressions.erase(aggr.expressions.begin() + i);
			i--;
			// we need to remap any references to this aggregate so they point to the other aggregate
			ColumnBinding original_binding(aggr.aggregate_index, original_index);
			ColumnBinding new_binding(aggr.aggregate_index, entry->second);
			aggregate_map[original_binding] = new_binding;
		}
	}
}

} // namespace duckdb
