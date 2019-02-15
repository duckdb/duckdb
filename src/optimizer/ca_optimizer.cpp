#include "optimizer/ca_optimizer.hpp"

#include "parser/expression/bound_columnref_expression.hpp"

#include "planner/operator/logical_filter.hpp"
#include "planner/operator/logical_projection.hpp"

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

LogicalAggregate* CommonAggregateOptimizer::find_logical_aggregate(const vector<unique_ptr<LogicalOperator>>& child_operators) {
		for (auto& child_operator: child_operators) {

			if (child_operator->type == LogicalOperatorType::AGGREGATE_AND_GROUP_BY)
				return static_cast<LogicalAggregate*>(child_operator.get()); 
		}

		return nullptr;
}

void CommonAggregateOptimizer::ExtractCommonAggregateExpressions(LogicalOperator &projection) {

	auto aggregate = find_logical_aggregate(projection.children);

	// TODO: should I assert that size of projection.expressions and aggregate.groups + aggregate.expressions are equal?

	if (!aggregate) {
		return;
	}

	// TODO: Make this work in the case of the presence of groups.
	// auto nr_of_groups = aggregate->groups.size();

	aggregate_to_projection_map_t aggregate_to_projection_map;

	for (size_t i = 0; i < projection.expressions.size(); i++) {
		auto& positions = aggregate_to_projection_map[aggregate->expressions[i].get()];
		positions.insert(positions.end(), i);
	}

	vector<unique_ptr<Expression>> new_group_by_expressions;
	vector<unique_ptr<Expression>> new_projection_expressions(projection.expressions.size());

	size_t projection_index = 0;

	for (auto& aggregate_to_projections : aggregate_to_projection_map) {
		auto& positions = aggregate_to_projections.second;

		auto it = positions.begin();

		unique_ptr<Expression> bce = projection.expressions[*it]->Copy();

		// TODO: do we need to assert that bce is a BoundColumnRefExpression?

		static_cast<BoundColumnRefExpression&>(*bce).binding.column_index = projection_index;

		while (it != positions.end()) {
			new_projection_expressions[*it] = bce->Copy();
			it++;
		}

		Expression* aggregate_expression = aggregate_to_projections.first;

		new_group_by_expressions.push_back(aggregate_expression->Copy());
		
		projection_index++;
	}

	projection.expressions.swap(new_projection_expressions);
	aggregate->expressions.swap(new_group_by_expressions);
}
