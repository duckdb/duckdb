#include "duckdb/optimizer/cse_optimizer.hpp"

#include "duckdb/planner/expression/common_subexpression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"

using namespace duckdb;
using namespace std;

void CommonSubExpressionOptimizer::VisitOperator(LogicalOperator &op) {
	switch (op.type) {
	case LogicalOperatorType::FILTER:
	case LogicalOperatorType::PROJECTION:
		ExtractCommonSubExpresions(op);
		break;
	default:
		break;
	}
	LogicalOperatorVisitor::VisitOperator(op);
}

void CommonSubExpressionOptimizer::CountExpressions(Expression &expr, expression_map_t<CSENode> &expression_count) {
	// we only consider expressions with children for CSE elimination
	switch (expr.expression_class) {
	case ExpressionClass::BOUND_COLUMN_REF:
	case ExpressionClass::BOUND_CONSTANT:
	case ExpressionClass::BOUND_PARAMETER:
	case ExpressionClass::COMMON_SUBEXPRESSION:
		return;
	default:
		break;
	}
	auto node = expression_count.find(&expr);
	if (node == expression_count.end()) {
		// first time we encounter this expression, insert this node with [count = 1]
		expression_count[&expr] = CSENode(1);
	} else {
		// we encountered this expression before, increment the occurrence count
		node->second.count++;
	}
	// recursively count the children
	ExpressionIterator::EnumerateChildren(expr, [&](Expression &child) { CountExpressions(child, expression_count); });
}

void CommonSubExpressionOptimizer::PerformCSEReplacement(unique_ptr<Expression> *expr_ptr,
                                                         expression_map_t<CSENode> &expression_count) {
	Expression &expr = **expr_ptr;
	switch (expr.expression_class) {
	case ExpressionClass::BOUND_COLUMN_REF:
	case ExpressionClass::BOUND_CONSTANT:
	case ExpressionClass::BOUND_PARAMETER:
	case ExpressionClass::COMMON_SUBEXPRESSION:
		return;
	default:
		break;
	}
	// check if this child is eligible for CSE elimination
	if (expression_count.find(&expr) == expression_count.end()) {
		// no occurrence: skip the node
		return;
	}
	auto &node = expression_count[&expr];
	if (node.count > 1) {
		// this expression occurs more than once! replace it with a CSE
		// check if it has already been replaced with a CSE before
		auto alias = expr.alias.empty() ? expr.GetName() : expr.alias;
		if (!node.expr) {
			// the CSE does not exist yet: create the CSE with the ownership of this node
			node.expr = &expr;
			*expr_ptr = make_unique<CommonSubExpression>(move(*expr_ptr), alias);
		} else {
			// the CSE already exists: create a CSE referring to the existing node
			*expr_ptr = make_unique<CommonSubExpression>(node.expr, alias);
		}
		return;
	}
	// this expression only occurs once, we can't perform CSE elimination
	// look into the children to see if we can replace them
	ExpressionIterator::EnumerateChildren(expr, [&](unique_ptr<Expression> child) -> unique_ptr<Expression> {
		PerformCSEReplacement(&child, expression_count);
		return move(child);
	});
}

void CommonSubExpressionOptimizer::ExtractCommonSubExpresions(LogicalOperator &op) {
	// first we count for each expression with children how many types it occurs
	expression_map_t<CSENode> expression_count;
	for (auto &expr : op.expressions) {
		CountExpressions(*expr, expression_count);
	}
	// now we iterate over all the expressions and perform the actual CSE elimination
	for (idx_t i = 0; i < op.expressions.size(); i++) {
		PerformCSEReplacement(&op.expressions[i], expression_count);
	}
}
