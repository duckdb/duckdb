#include "parser/expression/bound_expression.hpp"
#include "parser/expression/cast_expression.hpp"
#include "parser/query_node/set_operation_node.hpp"
#include "planner/logical_plan_generator.hpp"
#include "planner/operator/list.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<LogicalOperator> LogicalPlanGenerator::CastSetOpToTypes(vector<TypeId> &source_types,
                                                                   vector<TypeId> &target_types,
                                                                   unique_ptr<LogicalOperator> op) {
	assert(op);
	// first check if we even need to cast
	assert(source_types.size() == target_types.size());
	if (source_types == target_types) {
		// source and target types are equal: don't need to cast
		return op;
	}
	// otherwise add casts
	auto node = op.get();
	if (node->type == LogicalOperatorType::PROJECTION) {
		// "node" is a projection; we can just do the casts in there
		assert(node->expressions.size() == source_types.size());
		// add the casts to the selection list
		for (size_t i = 0; i < target_types.size(); i++) {
			if (node->expressions[i]->return_type != target_types[i]) {
				// differing types, have to add a cast
				node->expressions[i] = make_unique<CastExpression>(target_types[i], move(node->expressions[i]));
			}
		}
		return op;
	} else {
		// found a non-projection operator
		// push a new projection containing the casts

		// first push a logical subquery
		auto subquery_index = binder.GenerateTableIndex();
		auto subquery = make_unique<LogicalSubquery>(move(op), subquery_index);

		assert(subquery->column_count == source_types.size());

		// now generate the expression list
		vector<unique_ptr<Expression>> select_list;
		for (size_t i = 0; i < target_types.size(); i++) {
			unique_ptr<Expression> result = make_unique<BoundColumnRefExpression>("", source_types[i], ColumnBinding(subquery_index, i));
			if (source_types[i] != target_types[i]) {
				// add a cast only if the source and target types are not equivalent
				result = make_unique<CastExpression>(target_types[i], move(result));
			}
			select_list.push_back(move(result));
		}
		auto projection = make_unique<LogicalProjection>(binder.GenerateTableIndex(), move(select_list));
		projection->children.push_back(move(subquery));
		return move(projection);
	}
}

void LogicalPlanGenerator::CreatePlan(SetOperationNode &statement) {
	auto &binding = statement.binding;

	// Generate the logical plan for the left and right sides of the set operation
	LogicalPlanGenerator generator_left(*binding.left_binder, context);
	LogicalPlanGenerator generator_right(*binding.right_binder, context);

	generator_left.CreatePlan(*statement.left);
	auto left_node = move(generator_left.root);

	generator_right.CreatePlan(*statement.right);
	auto right_node = move(generator_right.root);

	// for both the left and right sides, cast them to the same types
	left_node = CastSetOpToTypes(statement.left->types, statement.types, move(left_node));
	right_node = CastSetOpToTypes(statement.right->types, statement.types, move(right_node));
	
	// create actual logical ops for setops
	LogicalOperatorType logical_type;
	switch (statement.setop_type) {
	case SetOperationType::UNION:
		logical_type = LogicalOperatorType::UNION;
		break;
	case SetOperationType::EXCEPT:
		logical_type = LogicalOperatorType::EXCEPT;
		break;
	default:
		assert(statement.setop_type == SetOperationType::INTERSECT);
		logical_type = LogicalOperatorType::INTERSECT;
		break;
	}
	root = make_unique<LogicalSetOperation>(binding.setop_index, statement.types.size(), move(left_node), move(right_node), logical_type);

	VisitQueryNode(statement);
}
