

#include "planner/operator/logical_join.hpp"
#include "planner/operator/logical_filter.hpp"

#include "parser/expression/expression_list.hpp"

using namespace duckdb;
using namespace std;

JoinSide LogicalJoin::GetJoinSide(std::unique_ptr<AbstractExpression> &expr) {
	if (expr->type == ExpressionType::COLUMN_REF) {
		auto colref = (ColumnRefExpression *)expr.get();
		if (children[0]->referenced_tables.find(colref->binding.table_index) !=
		    children[0]->referenced_tables.end()) {
			return JoinSide::LEFT;
		} else {
			assert(children[1]->referenced_tables.find(
			           colref->binding.table_index) !=
			       children[1]->referenced_tables.end());
			return JoinSide::RIGHT;
		}
	} else {
		JoinSide join_side = JoinSide::NONE;
		for (auto &child : expr->children) {
			auto child_side = GetJoinSide(child);
			if (child_side != join_side) {
				join_side =
				    join_side == JoinSide::NONE ? child_side : JoinSide::BOTH;
			}
		}
		return join_side;
	}
}

void LogicalJoin::SetJoinCondition(
    std::unique_ptr<AbstractExpression> condition) {
	assert(children.size() == 2);
	if (condition->GetExpressionType() == ExpressionType::CONJUNCTION_AND) {
		// traverse down the expression tree along conjunction
		for (auto &child : condition->children) {
			SetJoinCondition(move(child));
		}
	} else if (condition->GetExpressionType() >=
	               ExpressionType::COMPARE_EQUAL &&
	           condition->GetExpressionType() <=
	               ExpressionType::COMPARE_GREATERTHANOREQUALTO) {
		// logical comparison
		// figure out which side belongs to the left and which side belongs to
		// the right
		assert(condition->children.size() == 2);
		size_t left_side = GetJoinSide(condition->children[0]);
		size_t right_side = GetJoinSide(condition->children[1]);

		JoinCondition join_condition;
		join_condition.comparison = condition->GetExpressionType();
		if (left_side == JoinSide::LEFT && right_side == JoinSide::RIGHT) {
			// left is left right is right
			join_condition.left = move(condition->children[0]);
			join_condition.right = move(condition->children[1]);
		} else if (left_side == JoinSide::RIGHT &&
		           right_side == JoinSide::LEFT) {
			// left is right right is left
			join_condition.left = move(condition->children[1]);
			join_condition.right = move(condition->children[0]);
		} else {
			// create filters for non-comparision conditions
			auto filter = make_unique<LogicalFilter>(move(condition));
			if (left_side == JoinSide::LEFT || right_side == JoinSide::LEFT) {
				filter->AddChild(move(children[0]));
				children[0] = move(filter);
			} else {
				// if neither side is part of the join we push to right side as
				// well because whatever
				filter->AddChild(move(children[1]));
				children[1] = move(filter);
			}
			return;
		}
		conditions.push_back(move(join_condition));

	} else if (condition->GetExpressionType() == ExpressionType::OPERATOR_NOT) {
		assert(condition->children.size() == 1);
		ExpressionType child_type = condition->children[0]->GetExpressionType();

		if (child_type < ExpressionType::COMPARE_EQUAL ||
		    child_type > ExpressionType::COMPARE_GREATERTHANOREQUALTO) {
			throw Exception("ON NOT only supports comparision operators");
		}
		// switcheroo the child condition
		// our join needs to compare explicit left and right sides. So we invert
		// the condition to express NOT, this way we can still use equi-joins

		ExpressionType negated_type = ExpressionType::INVALID;
		// TODO: this could be useful elsewhere?
		switch (child_type) {
		case ExpressionType::COMPARE_EQUAL:
			negated_type = ExpressionType::COMPARE_NOTEQUAL;
			break;
		case ExpressionType::COMPARE_NOTEQUAL:
			negated_type = ExpressionType::COMPARE_EQUAL;
			break;
		case ExpressionType::COMPARE_LESSTHAN:
			negated_type = ExpressionType::COMPARE_GREATERTHANOREQUALTO;
			break;
		case ExpressionType::COMPARE_GREATERTHAN:
			negated_type = ExpressionType::COMPARE_LESSTHANOREQUALTO;
			break;
		case ExpressionType::COMPARE_LESSTHANOREQUALTO:
			negated_type = ExpressionType::COMPARE_GREATERTHAN;
			break;
		case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
			negated_type = ExpressionType::COMPARE_LESSTHAN;
			break;

		default:
			throw Exception("Unsupported join criteria in negation");
		}

		condition->children[0]->type = negated_type;
		SetJoinCondition(move(condition->children[0]));
	}
}
