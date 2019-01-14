#include "parser/expression/cast_expression.hpp"
#include "parser/expression/bound_expression.hpp"
#include "parser/query_node/set_operation_node.hpp"
#include "planner/logical_plan_generator.hpp"
#include "planner/operator/list.hpp"

using namespace duckdb;
using namespace std;

static unique_ptr<LogicalOperator> CastSetOpToTypes(vector<TypeId> &types, unique_ptr<LogicalOperator> op) {
	auto node = op.get();
	while (!IsProjection(node->type) && node->children.size() == 1) {
		node = node->children[0].get();
	}
	if (node->type == LogicalOperatorType::PROJECTION) {
		// found a projection node, we can just do the casts in there
		assert(node->expressions.size() == types.size());
		// add the casts to the selection list
		for (size_t i = 0; i < types.size(); i++) {
			if (node->expressions[i]->return_type != types[i]) {
				// differing types, have to add a cast
				node->expressions[i] = make_unique<CastExpression>(types[i], move(node->expressions[i]));
			}
		}
		return op;
	} else {
		// found a UNION or other set operator before we found a projection
		// we need to push a projection IF we need to do any casts
		// first check if we need to do any
		assert(node == op.get()); // if we don't encounter a projection the union should be the root node
		node->ResolveOperatorTypes();
		assert(types.size() == node->types.size());
		bool require_cast = false;
		for (size_t i = 0; i < types.size(); i++) {
			if (node->types[i] != types[i]) {
				require_cast = true;
				break;
			}
		}
		if (!require_cast) {
			// no cast required
			return op;
		}
		// need to perform a cast, push a projection
		vector<unique_ptr<Expression>> select_list;
		select_list.reserve(types.size());
		for (size_t i = 0; i < types.size(); i++) {
			unique_ptr<Expression> result = make_unique<BoundExpression>(node->types[i], i);
			if (node->types[i] != types[i]) {
				result = make_unique<CastExpression>(types[i], move(result));
			}
			select_list.push_back(move(result));
		}
		auto projection = make_unique<LogicalProjection>(move(select_list));
		projection->children.push_back(move(op));
		projection->ResolveOperatorTypes();
		return move(projection);
	}
}

void LogicalPlanGenerator::CreatePlan(SetOperationNode &statement) {
	// Generate the logical plan for the left and right sides of the set operation
	LogicalPlanGenerator generator_left(context, *statement.setop_left_binder);
	LogicalPlanGenerator generator_right(context, *statement.setop_right_binder);

	// get the projections
	auto &left_select_list = statement.left->GetSelectList();
	auto &right_select_list = statement.right->GetSelectList();
	if (left_select_list.size() != right_select_list.size()) {
		throw Exception("Set operations can only apply to expressions with the "
		                "same number of result columns");
	}

	vector<TypeId> union_types;
	// figure out the types of the setop result from the selection lists
	for (size_t i = 0; i < left_select_list.size(); i++) {
		Expression *proj_ele = left_select_list[i].get();

		TypeId union_expr_type = TypeId::INVALID;
		auto left_expr_type = proj_ele->return_type;
		auto right_expr_type = right_select_list[i]->return_type;
		// the type is the biggest of the two types
		// we might need to cast one of the sides
		union_expr_type = left_expr_type;
		if (right_expr_type > union_expr_type) {
			union_expr_type = right_expr_type;
		}

		union_types.push_back(union_expr_type);
	}

	// now visit the expressions to generate the plans
	generator_left.CreatePlan(*statement.left);
	auto left_node = move(generator_left.root);

	generator_right.CreatePlan(*statement.right);
	auto right_node = move(generator_right.root);

	assert(left_node);
	assert(right_node);

	// check for each node if we need to cast any of the columns
	left_node = CastSetOpToTypes(union_types, move(left_node));
	right_node = CastSetOpToTypes(union_types, move(right_node));

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
