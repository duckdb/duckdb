#include "duckdb/optimizer/ca_optimizer.hpp"

#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"

using namespace duckdb;
using namespace std;

void CommonAggregateOptimizer::VisitOperator(LogicalOperator &op) {
	switch (op.type) {
	case LogicalOperatorType::PROJECTION:
		ExtractCommonAggregateExpressions(op);
		break;
	default:
		break;
	}
	LogicalOperatorVisitor::VisitOperator(op);
}

/*
P := PROJECTION
W := WINDOW
F := FILTER
A := AGGREGATE_AND_GROUP_BY

POSSIBLE AGGREGATION OPERATOR PIPELINES:
P A
P W F A
P W A
P F A
*/
LogicalAggregate *CommonAggregateOptimizer::find_logical_aggregate(vector<Expression *> &expressions,
                                                                   LogicalOperator &projection) {
	LogicalOperator *node = &projection;

	for (auto &expression : projection.expressions) {
		expressions.push_back(expression.get());
	}

	for (auto &child_operator : node->children) {
		if (child_operator->type == LogicalOperatorType::WINDOW) {
			node = child_operator.get();
			for (auto &expression : node->expressions) {
				expressions.push_back(expression.get());
			}
		}
	}

	for (auto &child_operator : node->children) {
		if (child_operator->type == LogicalOperatorType::FILTER) {
			node = child_operator.get();
			for (auto &expression : node->expressions) {
				expressions.push_back(expression.get());
			}
		}
	}

	for (auto &child_operator : node->children) {
		if (child_operator->type == LogicalOperatorType::AGGREGATE_AND_GROUP_BY) {
			return static_cast<LogicalAggregate *>(child_operator.get());
		}
	}

	return nullptr;
}

void CommonAggregateOptimizer::find_bound_references(Expression &expression, const LogicalAggregate &aggregate,
                                                     aggregate_to_bound_ref_map_t &aggregate_to_projection_map) {
	if (expression.GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
		auto &binding = static_cast<BoundColumnRefExpression &>(expression).binding;

		if (binding.table_index == aggregate.aggregate_index) {
			// this column_expression represents an aggregate: start doing some bookkeeping.
			auto &positions = aggregate_to_projection_map[aggregate.expressions[binding.column_index].get()];
			positions.push_back(&binding.column_index);
		}
	}

	ExpressionIterator::EnumerateChildren(expression, [&](Expression &expression) {
		find_bound_references(expression, aggregate, aggregate_to_projection_map);
	});
}

void CommonAggregateOptimizer::ExtractCommonAggregateExpressions(LogicalOperator &projection) {
	vector<Expression *> operator_chain_expressions;

	auto aggregate = find_logical_aggregate(operator_chain_expressions, projection);

	if (!aggregate) {
		return;
	}

	vector<unique_ptr<Expression>> new_aggregate_expressions;

	aggregate_to_bound_ref_map_t aggregate_to_projection_map;

	for (auto &column_expression : operator_chain_expressions) {
		find_bound_references(*column_expression, *aggregate, aggregate_to_projection_map);
	}

	// indices to aggregates start after indices to groups.
	index_t aggregate_index = 0;

	for (auto &aggregate_to_projections : aggregate_to_projection_map) {
		auto &positions = aggregate_to_projections.second;

		for (auto index_ptr : positions) {
			*index_ptr = aggregate_index;
		}

		Expression *aggregate_expression = aggregate_to_projections.first;

		new_aggregate_expressions.push_back(aggregate_expression->Copy());

		aggregate_index++;
	}
	// TODO this fixes sqlite issues but is probably not the underlying issue
	if (new_aggregate_expressions.size() > 0) {
		aggregate->expressions.swap(new_aggregate_expressions);
	}
}
