#include "optimizer/cse_optimizer.hpp"

#include "planner/operator/logical_filter.hpp"
#include "planner/operator/logical_projection.hpp"

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

void CommonSubExpressionOptimizer::CountExpressions(Expression *expr, expression_map_t<CSENode> &expression_count) {
	if (expr->ChildCount() > 0) {
		// we only consider expressions with children for CSE elimination
		auto node = expression_count.find(expr);
		if (node == expression_count.end()) {
			// first time we encounter this expression, insert this node with [count = 1]
			expression_count[expr] = CSENode(1);
		} else {
			// we encountered this expression before, increment the occurrence count
			node->second.count++;
		}
		// recursively count the children
		expr->EnumerateChildren([&](Expression *child) { CountExpressions(child, expression_count); });
	}
}

Expression *CommonSubExpressionOptimizer::PerformCSEReplacement(Expression *expr, expression_map_t<CSENode> &expression_count) {
	if (expr->ChildCount() > 0) {
		// check if this child is eligible for CSE elimination
		if (expression_count.find(expr) == expression_count.end()) {
			return nullptr;
		}
		auto &node = expression_count[expr];
		if (node.count > 1) {
			// this expression occurs more than once! replace it with a CSE
			// check if it has already been replaced with a CSE before
			if (!node.expr) {
				// it has not! create the CSE with the ownership of this node
				node.expr = expr;
			}
			return node.expr;
		}
		// this expression only occurs once, we can't perform CSE elimination
		// look into the children to see if we can replace them
		for (size_t i = 0, child_count = expr->ChildCount(); i < child_count; i++) {
			auto child = expr->GetChild(i);
			auto cse_replacement = PerformCSEReplacement(child, expression_count);
			if (cse_replacement) {
				// we can replace the child with a Common SubExpression
				auto alias = child->alias.empty() ? child->GetName() : child->alias;
				if (cse_replacement == child) {
					// we have to move the expression into the CSE because it is the first CSE created for this
					// expression
					expr->ReplaceChild(
					    [&](unique_ptr<Expression> expr) -> unique_ptr<Expression> {
						    return make_unique<CommonSubExpression>(move(expr), alias);
					    },
					    i);
				} else {
					// there already exists a CSE node for this expression
					expr->ReplaceChild(
					    [&](unique_ptr<Expression> expr) -> unique_ptr<Expression> {
						    return make_unique<CommonSubExpression>(cse_replacement, alias);
					    },
					    i);
				}
			}
		}
	}
	return nullptr;
	;
}

void CommonSubExpressionOptimizer::ExtractCommonSubExpresions(LogicalOperator &op) {
	// first we count for each expression with children how many types it occurs
	expression_map_t<CSENode> expression_count;
	for (auto &expr : op.expressions) {
		CountExpressions(expr.get(), expression_count);
	}
	// now we iterate over all the expressions and perform the actual CSE elimination
	for (size_t i = 0; i < op.expressions.size(); i++) {
		auto child = op.expressions[i].get();
		auto cse_replacement = PerformCSEReplacement(child, expression_count);
		if (cse_replacement) {
			auto alias = child->alias.empty() ? child->GetName() : child->alias;
			if (cse_replacement == child) {
				op.expressions[i] = make_unique<CommonSubExpression>(move(op.expressions[i]), alias);
			} else {
				op.expressions[i] = make_unique<CommonSubExpression>(cse_replacement, alias);
			}
		}
	}
}
