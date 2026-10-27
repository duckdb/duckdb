#include "duckdb/optimizer/common_aggregate_optimizer.hpp"

#include "duckdb/parser/expression_map.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"

namespace duckdb {

CommonAggregateOptimizer::CommonAggregateOptimizer(bool preserve_arity) : preserve_arity(preserve_arity) {
}

void CommonAggregateOptimizer::StandardVisitOperator(LogicalOperator &op) {
	VisitOperatorChildren(op);
	if (!aggregate_map.empty()) {
		VisitOperatorExpressions(op);
	}
}

void CommonAggregateOptimizer::VisitOperator(LogicalOperator &op) {
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_PROJECTION: {
		// a projection absorbs binding remaps and fixes the column count of the chain above it
		CommonAggregateOptimizer common_aggregate(false);
		common_aggregate.StandardVisitOperator(op);
		return;
	}
	case LogicalOperatorType::LOGICAL_UNION:
	case LogicalOperatorType::LOGICAL_EXCEPT:
	case LogicalOperatorType::LOGICAL_INTERSECT:
	case LogicalOperatorType::LOGICAL_MATERIALIZED_CTE:
	case LogicalOperatorType::LOGICAL_RECURSIVE_CTE: {
		// children are consumed positionally (set operation columns, CTE scans):
		// binding remaps cannot reach those consumers, so the children's column count must not change
		for (auto &child : op.children) {
			CommonAggregateOptimizer child_optimizer(true);
			child_optimizer.VisitOperator(*child);
		}
		return;
	}
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
		// the aggregate consumes its child by binding - the child chain's column count is unconstrained
		auto parent_preserve_arity = preserve_arity;
		preserve_arity = false;
		StandardVisitOperator(op);
		preserve_arity = parent_preserve_arity;
		if (!preserve_arity) {
			// removing duplicate aggregates changes this operator's column count
			ExtractCommonAggregates(op.Cast<LogicalAggregate>());
		}
		return;
	}
	default:
		break;
	}

	StandardVisitOperator(op);
}

unique_ptr<Expression> CommonAggregateOptimizer::VisitReplace(BoundColumnRefExpression &expr,
                                                              unique_ptr<Expression> *expr_ptr) {
	// check if this column ref points to an aggregate that was remapped; if it does we remap it
	auto entry = aggregate_map.find(expr.Binding());
	if (entry != aggregate_map.end()) {
		expr.BindingMutable() = entry->second;
	}
	return nullptr;
}

void CommonAggregateOptimizer::ExtractCommonAggregates(LogicalAggregate &aggr) {
	expression_map_t<ProjectionIndex> aggregate_remap;
	idx_t total_erased = 0;
	for (idx_t i = 0; i < aggr.expressions.size(); i++) {
		ProjectionIndex original_index(i + total_erased);
		ProjectionIndex new_index(i);
		// volatile aggregates must not be deduplicated: each call is independent
		if (aggr.expressions[i]->IsVolatile()) {
			if (new_index != original_index) {
				ColumnBinding original_binding(aggr.aggregate_index, original_index);
				ColumnBinding new_binding(aggr.aggregate_index, new_index);
				aggregate_map[original_binding] = new_binding;
			}
			continue;
		}
		auto entry = aggregate_remap.find(*aggr.expressions[i]);
		if (entry == aggregate_remap.end()) {
			// aggregate does not exist yet: add it to the map
			aggregate_remap[*aggr.expressions[i]] = new_index;
			if (new_index != original_index) {
				// this aggregate is not erased, however an aggregate BEFORE it has been erased
				// so we need to remap this aggregate
				ColumnBinding original_binding(aggr.aggregate_index, original_index);
				ColumnBinding new_binding(aggr.aggregate_index, new_index);
				aggregate_map[original_binding] = new_binding;
			}
		} else {
			// aggregate already exists! we can remove this entry
			total_erased++;
			aggr.expressions.erase_at(i);
			i--;
			// we need to remap any references to this aggregate so they point to the other aggregate
			ColumnBinding original_binding(aggr.aggregate_index, original_index);
			ColumnBinding new_binding(aggr.aggregate_index, entry->second);
			aggregate_map[original_binding] = new_binding;
		}
	}
}

} // namespace duckdb
