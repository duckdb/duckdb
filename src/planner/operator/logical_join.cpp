#include "planner/operator/logical_join.hpp"

#include "parser/expression/list.hpp"
#include "planner/operator/logical_filter.hpp"

using namespace duckdb;
using namespace std;

vector<string> LogicalJoin::GetNames() {
	auto left = children[0]->GetNames();
	if (type != JoinType::SEMI && type != JoinType::ANTI) {
		// for normal joins we project both sides
		auto right = children[1]->GetNames();
		left.insert(left.end(), right.begin(), right.end());
	}
	return left;
}

void LogicalJoin::ResolveTypes() {
	types.insert(types.end(), children[0]->types.begin(), children[0]->types.end());
	if (type != JoinType::SEMI && type != JoinType::ANTI) {
		// for normal joins we project both sides
		types.insert(types.end(), children[1]->types.begin(), children[1]->types.end());
	}
}

std::string LogicalJoin::ParamsToString() const {
	std::string result = "";
	if (conditions.size() > 0) {
		result += "[";
		for (size_t i = 0; i < conditions.size(); i++) {
			auto &cond = conditions[i];
			result += ExpressionTypeToString(cond.comparison) + "(" + cond.left->ToString() + ", " +
			          cond.right->ToString() + ")";
			if (i < conditions.size() - 1) {
				result += ", ";
			}
		}
		result += "]";
	}

	return result;
}

JoinSide LogicalJoin::GetJoinSide(LogicalOperator *op, std::unique_ptr<Expression> &expr) {
	if (expr->type == ExpressionType::SELECT_SUBQUERY) {
		return JoinSide::BOTH;
	}
	if (expr->type == ExpressionType::COLUMN_REF) {
		auto colref = (ColumnRefExpression *)expr.get();
		if (op->children[0]->referenced_tables.find(colref->binding.table_index) !=
		    op->children[0]->referenced_tables.end()) {
			return JoinSide::LEFT;
		} else if (op->children[1]->referenced_tables.find(colref->binding.table_index) !=
		           op->children[1]->referenced_tables.end()) {
			return JoinSide::RIGHT;
		} else {
			// if its neither in left nor right its both not none
			return JoinSide::BOTH;
		}

	} else {
		JoinSide join_side = JoinSide::NONE;
		for (auto &child : expr->children) {
			auto child_side = LogicalJoin::GetJoinSide(op, child);
			if (child_side != join_side && child_side != JoinSide::NONE) {
				join_side = join_side == JoinSide::NONE ? child_side : JoinSide::BOTH;
			}
		}
		return join_side;
	}
}

ExpressionType LogicalJoin::NegateComparisionExpression(ExpressionType type) {
	ExpressionType negated_type = ExpressionType::INVALID;
	switch (type) {
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
	return negated_type;
}

ExpressionType LogicalJoin::FlipComparisionExpression(ExpressionType type) {
	ExpressionType flipped_type = ExpressionType::INVALID;
	switch (type) {
	case ExpressionType::COMPARE_NOTEQUAL:
	case ExpressionType::COMPARE_EQUAL:
		flipped_type = type;
		break;
	case ExpressionType::COMPARE_LESSTHAN:
		flipped_type = ExpressionType::COMPARE_GREATERTHAN;
		break;
	case ExpressionType::COMPARE_GREATERTHAN:
		flipped_type = ExpressionType::COMPARE_LESSTHAN;
		break;
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		flipped_type = ExpressionType::COMPARE_GREATERTHANOREQUALTO;
		break;
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		flipped_type = ExpressionType::COMPARE_LESSTHANOREQUALTO;
		break;

	default:
		throw Exception("Unsupported join criteria in flip");
	}
	return flipped_type;
}

void LogicalJoin::SetJoinCondition(std::unique_ptr<Expression> condition) {
	assert(children.size() == 2);
	if (condition->GetExpressionType() == ExpressionType::CONJUNCTION_AND) {
		// traverse down the expression tree along conjunction
		for (auto &child : condition->children) {
			SetJoinCondition(move(child));
		}
	} else {
		auto total_side = LogicalJoin::GetJoinSide(this, condition);
		if (total_side == JoinSide::LEFT || total_side == JoinSide::RIGHT || total_side == JoinSide::NONE) {
			// the condition only relates to one side
			// turn it into a filter
			auto filter = make_unique<LogicalFilter>(move(condition));
			if (total_side == JoinSide::LEFT) {
				filter->AddChild(move(children[0]));
				children[0] = move(filter);
			} else {
				// if neither side is part of the join we push to right side as
				// well because whatever
				filter->AddChild(move(children[1]));
				children[1] = move(filter);
			}
		} else if (condition->GetExpressionType() >= ExpressionType::COMPARE_EQUAL &&
		           condition->GetExpressionType() <= ExpressionType::COMPARE_NOTLIKE) {
			// logical comparison
			// figure out which side belongs to the left and which side belongs
			// to the right
			assert(condition->children.size() == 2);
			size_t left_side = LogicalJoin::GetJoinSide(this, condition->children[0]);
			size_t right_side = LogicalJoin::GetJoinSide(this, condition->children[1]);

			JoinCondition join_condition;
			join_condition.comparison = condition->GetExpressionType();
			if (left_side == JoinSide::LEFT && right_side == JoinSide::RIGHT) {
				// left is left right is right
				join_condition.left = move(condition->children[0]);
				join_condition.right = move(condition->children[1]);
			} else if (left_side == JoinSide::RIGHT && right_side == JoinSide::LEFT) {
				// left is right right is left
				join_condition.left = move(condition->children[1]);
				join_condition.right = move(condition->children[0]);
				// have to negate the condition, too
				join_condition.comparison = FlipComparisionExpression(join_condition.comparison);
			} else {
				// this can't happen, we handle this before
				assert(0);
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
			// our join needs to compare explicit left and right sides. So we
			// invert the condition to express NOT, this way we can still use
			// equi-joins

			condition->children[0]->type = NegateComparisionExpression(child_type);
			SetJoinCondition(move(condition->children[0]));
		} else {
			// unrecognized type
			throw Exception("Unrecognized operator type for join!");
		}
	}
}
