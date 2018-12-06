#include "optimizer/logical_rules/remove_obsolete_filters.hpp"

#include <optimizer/rule.hpp>

using namespace duckdb;
using namespace std;

RemoveObsoleteFilterRule::RemoveObsoleteFilterRule() {
	// we match on a filter with at least two equality/gt/lt comparisons
	vector<ExpressionType> matched_types = {
	    ExpressionType::COMPARE_GREATERTHAN, ExpressionType::COMPARE_GREATERTHANOREQUALTO,
	    ExpressionType::COMPARE_LESSTHAN, ExpressionType::COMPARE_LESSTHANOREQUALTO, ExpressionType::COMPARE_EQUAL};
	auto compare_one = make_unique_base<AbstractRuleNode, ExpressionNodeSet>(matched_types);
	auto compare_two = make_unique_base<AbstractRuleNode, ExpressionNodeSet>(matched_types);

	root = make_unique_base<AbstractRuleNode, LogicalNodeType>(LogicalOperatorType::FILTER);

	root->children.push_back(move(compare_one));
	root->children.push_back(move(compare_two));
	root->child_policy = ChildPolicy::SOME;
}

static int GetConstantComparison(Expression *expr) {
	if (expr->children[0]->type == ExpressionType::VALUE_CONSTANT) {
		return 0;
	}
	if (expr->children[1]->type == ExpressionType::VALUE_CONSTANT) {
		return 1;
	}
	return -1;
}

static bool IsGreaterThan(ExpressionType type) {
	return type == ExpressionType::COMPARE_GREATERTHAN || type == ExpressionType::COMPARE_GREATERTHANOREQUALTO;
}

static bool IsLessThan(ExpressionType type) {
	return type == ExpressionType::COMPARE_LESSTHAN || type == ExpressionType::COMPARE_LESSTHANOREQUALTO;
}

unique_ptr<LogicalOperator> RemoveObsoleteFilterRule::Apply(Rewriter &rewriter, LogicalOperator &op_root,
                                                            vector<AbstractOperator> &bindings, bool &fixed_point) {
	auto &filter = bindings[0].value.op;
	auto &compare_one = bindings[1].value.expr;
	auto &compare_two = bindings[2].value.expr;

	// we only support numeric checks for now
	if (!TypeIsNumeric(compare_one->children[0]->return_type) ||
	    !TypeIsNumeric(compare_two->children[0]->return_type)) {
		return nullptr;
	}

	// check if these are comparisons with constants
	int constant_left_side = GetConstantComparison(compare_one);
	int constant_right_side = GetConstantComparison(compare_two);
	if (constant_left_side < 0 || constant_right_side < 0) {
		// not constant comparisons
		return nullptr;
	}
	// check if the other expression is identical for both comparisons
	if (!compare_one->children[1 - constant_left_side]->Equals(compare_two->children[1 - constant_right_side].get())) {
		// children are not equal
		return nullptr;
	}
	auto left_constant = ((ConstantExpression *)compare_one->children[constant_left_side].get())->value;
	auto right_constant = ((ConstantExpression *)compare_two->children[constant_right_side].get())->value;
	// get the expression types
	// potentially flip the expression to make them point the same way
	auto compare_type_left = constant_left_side == 0
	                             ? ComparisonExpression::FlipComparisionExpression(compare_one->type)
	                             : compare_one->type;
	auto compare_type_right = constant_right_side == 0
	                              ? ComparisonExpression::FlipComparisionExpression(compare_two->type)
	                              : compare_two->type;
	int discard = -1;
	if (compare_type_left == ExpressionType::COMPARE_EQUAL) {
		// the left side is an equality expression
		// we can remove the right side node because it can never be more selective
		discard = 1;
	} else if (compare_type_right == ExpressionType::COMPARE_EQUAL) {
		// the right side is an equality expression
		// we can remove the left side node because it can never be more selective
		discard = 0;
	} else if (IsGreaterThan(compare_type_left) && IsGreaterThan(compare_type_right)) {
		// both expressions point the same way, we can eliminate one of them
		// pick the highest expression
		if (left_constant > right_constant) {
			// use the left side
			discard = 1;
		} else if (left_constant == right_constant) {
			// the value is equal
			// we might need to pick whichever has the greater than
			if (compare_type_left == ExpressionType::COMPARE_GREATERTHAN) {
				// greater than is more selective than >=, so we use the left side
				discard = 1;
			} else {
				discard = 0;
			}
		} else {
			discard = 0;
		}

	} else if (IsLessThan(compare_type_left) && IsLessThan(compare_type_right)) {
		// both expressions point the same way, we can eliminate one of them
		// pick the lowest expression
		if (left_constant < right_constant) {
			// use the left side
			discard = 1;
		} else if (left_constant == right_constant) {
			// the value is equal
			// we might need to pick whichever has the less than
			if (compare_type_left == ExpressionType::COMPARE_LESSTHAN) {
				// less than is more selective than <=, so we use the left side
				discard = 1;
			} else {
				discard = 0;
			}
		} else {
			discard = 0;
		}
	}
	if (discard < 0) {
		return nullptr;
	}
	auto discarded_expression = discard == 0 ? compare_one : compare_two;
	// finally we remove the original in-expression from the filter
	for (size_t i = 0; i < filter->expressions.size(); i++) {
		if (filter->expressions[i].get() == discarded_expression) {
			filter->expressions.erase(filter->expressions.begin() + i);
			break;
		}
	}
	return nullptr;
}
