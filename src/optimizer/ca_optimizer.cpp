#include "optimizer/ca_optimizer.hpp"

#include "parser/expression/bound_expression.hpp"

#include "planner/operator/logical_filter.hpp"
#include "planner/operator/logical_projection.hpp"

using namespace duckdb;
using namespace std;

#include <iostream>

/* boss case
SELECT i, SUM(i), SUM(i), COUNT(*), AVG(i), j
	FROM foo
	GROUP BY i, j
	HAVING AVG(i) > 10 AND AVG(i) < 40;

PROJECTION[#0, #2, #3, #4, CAST[DECIMAL](#5) / CAST[DECIMAL](#6), #1](
    FILTER[CAST[DECIMAL](#9) / CAST[DECIMAL](#10)<40.000000, CAST[DECIMAL](#7) / CAST[DECIMAL](#8)>10.000000](
        AGGREGATE_AND_GROUP_BY
			[
            SUM(0.0),
            SUM(0.0),
            COUNT(*),
            SUM(0.0),
            COUNT(CAST[BIGINT](0.0)),
            SUM(0.0),
            COUNT(CAST[BIGINT](0.0)),
            SUM(0.0),
            COUNT(CAST[BIGINT](0.0))
			]
            [
				0.0,
				0.1
			] (GET(foo)
        )
    )
)
*/

// TODO: Check behavior of aliases and perhaps create a bug report for the having clause containing a alias of cast typed.

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
LogicalAggregate* CommonAggregateOptimizer::find_logical_aggregate(vector<Expression*>& expressions, LogicalOperator& projection) {

	LogicalOperator* node = &projection;

	for (auto& expression : projection.expressions)	{
		expressions.push_back(expression.get());
	}

	for (auto& child_operator: node->children) {
		if (child_operator->type == LogicalOperatorType::WINDOW) {
			node = child_operator.get();
			for (auto& expression : node->expressions) {
				expressions.push_back(expression.get());
			}
		}
	}

	for (auto& child_operator: node->children) {
		if (child_operator->type == LogicalOperatorType::FILTER) {
			node = child_operator.get();
			for (auto& expression : node->expressions) {
				expressions.push_back(expression.get());
			}
		}
	}

	for (auto& child_operator: node->children) {
		if (child_operator->type == LogicalOperatorType::AGGREGATE_AND_GROUP_BY) {
			return static_cast<LogicalAggregate*>(child_operator.get());
		}
	}

	return nullptr;
}

void CommonAggregateOptimizer::find_bound_references(Expression& expression, const LogicalAggregate& aggregate, aggregate_to_bound_ref_map_t& aggregate_to_projection_map, size_t& nr_of_groups) {
	if (expression.GetExpressionClass() == ExpressionClass::BOUND_REF) {

		auto column_index = &(static_cast<BoundExpression&>(expression).index);

		if (*column_index >= nr_of_groups) {
			// this column_expression represents an aggregate: start doing some bookkeeping.
			auto& positions = aggregate_to_projection_map[aggregate.expressions[*column_index - nr_of_groups].get()];
			positions.push_back(column_index);
		}
	}

	expression.EnumerateChildren(
		[this, &aggregate, &aggregate_to_projection_map, &nr_of_groups]
		(Expression *expression) {find_bound_references(*expression, aggregate, aggregate_to_projection_map, nr_of_groups);});
}

void CommonAggregateOptimizer::ExtractCommonAggregateExpressions(LogicalOperator& projection) {
	/* REMOVE CODE AFTER DEBUGGING
	std::cout << "BEFORE OPTIMIZING:" << std::endl;
	std::cout << projection.ToString() << std::endl;
	** REMOVE CODE AFTER DEBUGGING
	*/

	vector<Expression*> operator_chain_expressions;

	auto aggregate = find_logical_aggregate(operator_chain_expressions, projection);

	if (!aggregate) {
		return;
	}

	vector<unique_ptr<Expression>> new_aggregate_expressions;

	auto nr_of_groups = aggregate->groups.size();

	aggregate_to_bound_ref_map_t aggregate_to_projection_map;

	for (auto& column_expression : operator_chain_expressions) {
		find_bound_references(*column_expression, *aggregate, aggregate_to_projection_map, nr_of_groups);
	}

	// indices to aggregates start after indices to groups.
	size_t aggregate_index = nr_of_groups;

	for (auto& aggregate_to_projections : aggregate_to_projection_map) {
		auto& positions = aggregate_to_projections.second;

		for (auto index_ptr : positions) {
			*index_ptr = aggregate_index;
		}

		Expression* aggregate_expression = aggregate_to_projections.first;

		new_aggregate_expressions.push_back(aggregate_expression->Copy());
		
		aggregate_index++;
	}
	/* REMOVE CODE AFTER DEBUGGING
	aggregate->expressions.swap(new_aggregate_expressions);
	std::cout << "AFTER OPTIMIZING:" << std::endl;
	std::cout << projection.ToString() << std::endl;
	** REMOVE CODE AFTER DEBUGGING
	*/
}
