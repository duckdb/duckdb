#include "parser/expression/bound_expression.hpp"
#include "parser/expression/cast_expression.hpp"
#include "parser/query_node/set_operation_node.hpp"
#include "planner/logical_plan_generator.hpp"
#include "planner/operator/list.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<LogicalOperator> LogicalPlanGenerator::CastSetOpToTypes(vector<TypeId> &source_types, vector<TypeId> &target_types, unique_ptr<LogicalOperator> op) {
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
		assert(node->expressions.size() == target_types.size());
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

		// first generate the expression list
		vector<unique_ptr<Expression>> select_list;
		for (size_t i = 0; i < target_types.size(); i++) {
			unique_ptr<Expression> result = make_unique<BoundExpression>(source_types[i], i);
			if (source_types[i] != target_types[i]) {
				// add a cast only if the source and target types are not equivalent
				result = make_unique<CastExpression>(target_types[i], move(result));
			}
			select_list.push_back(move(result));
		}
		auto projection = make_unique<LogicalProjection>(bind_context.GenerateTableIndex(), move(select_list));
		projection->children.push_back(move(op));
		return move(projection);
	}
}

void LogicalPlanGenerator::CreatePlan(SetOperationNode &statement) {
	// Generate the logical plan for the left and right sides of the set operation
	LogicalPlanGenerator generator_left(context, *statement.binding.left_context);
	LogicalPlanGenerator generator_right(context, *statement.binding.right_context);

	generator_left.CreatePlan(*statement.left);
	auto left_node = move(generator_left.root);

	generator_right.CreatePlan(*statement.right);
	auto right_node = move(generator_right.root);

	// for both the left and right sides, cast them to the same types
	left_node = CastSetOpToTypes(statement.left->types, statement.types, move(left_node));
	right_node = CastSetOpToTypes(statement.right->types, statement.types, move(right_node));

	// create actual logical ops for setops
	switch (statement.setop_type) {
	case SetOperationType::UNION: {
		auto union_op = make_unique<LogicalUnion>(move(left_node), move(right_node));
		root = move(union_op);
		break;
	}
	case SetOperationType::EXCEPT: {
		auto except_op = make_unique<LogicalExcept>(move(left_node), move(right_node));
		root = move(except_op);
		break;
	}
	case SetOperationType::INTERSECT: {
		auto intersect_op = make_unique<LogicalIntersect>(move(left_node), move(right_node));
		root = move(intersect_op);
		break;
	}
	default:
		throw NotImplementedException("Set Operation type");
	}

	VisitQueryNode(statement);
}
