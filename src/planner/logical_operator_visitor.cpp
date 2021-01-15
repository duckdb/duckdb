#include "duckdb/planner/logical_operator_visitor.hpp"

#include "duckdb/planner/expression/list.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/list.hpp"

namespace duckdb {

void LogicalOperatorVisitor::VisitOperator(LogicalOperator &op) {
	VisitOperatorChildren(op);
	VisitOperatorExpressions(op);
}

void LogicalOperatorVisitor::VisitOperatorChildren(LogicalOperator &op) {
	for (auto &child : op.children) {
		VisitOperator(*child);
	}
}

void LogicalOperatorVisitor::EnumerateExpressions(LogicalOperator &op,
                                                  std::function<void(unique_ptr<Expression> *child)> callback) {
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_EXPRESSION_GET: {
		auto &get = (LogicalExpressionGet &)op;
		for (auto &expr_list : get.expressions) {
			for (auto &expr : expr_list) {
				callback(&expr);
			}
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_ORDER_BY: {
		auto &order = (LogicalOrder &)op;
		for (auto &node : order.orders) {
			callback(&node.expression);
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_TOP_N: {
		auto &order = (LogicalTopN &)op;
		for (auto &node : order.orders) {
			callback(&node.expression);
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_DISTINCT: {
		auto &distinct = (LogicalDistinct &)op;
		for (auto &target : distinct.distinct_targets) {
			callback(&target);
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_DELIM_JOIN:
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN: {
		if (op.type == LogicalOperatorType::LOGICAL_DELIM_JOIN) {
			auto &delim_join = (LogicalDelimJoin &)op;
			for (auto &expr : delim_join.duplicate_eliminated_columns) {
				callback(&expr);
			}
		}
		auto &join = (LogicalComparisonJoin &)op;
		for (auto &cond : join.conditions) {
			callback(&cond.left);
			callback(&cond.right);
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_ANY_JOIN: {
		auto &join = (LogicalAnyJoin &)op;
		callback(&join.condition);
		break;
	}
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
		auto &aggr = (LogicalAggregate &)op;
		for (idx_t i = 0; i < aggr.groups.size(); i++) {
			callback(&aggr.groups[i]);
		}
		break;
	}
	default:
		break;
	}
	for (idx_t i = 0; i < op.expressions.size(); i++) {
		callback(&op.expressions[i]);
	}
}

void LogicalOperatorVisitor::VisitOperatorExpressions(LogicalOperator &op) {
	LogicalOperatorVisitor::EnumerateExpressions(op, [&](unique_ptr<Expression> *child) { VisitExpression(child); });
}

void LogicalOperatorVisitor::VisitExpression(unique_ptr<Expression> *expression) {
	auto &expr = **expression;
	unique_ptr<Expression> result;
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::BOUND_AGGREGATE:
		result = VisitReplace((BoundAggregateExpression &)expr, expression);
		break;
	case ExpressionClass::BOUND_BETWEEN:
		result = VisitReplace((BoundBetweenExpression &)expr, expression);
		break;
	case ExpressionClass::BOUND_CASE:
		result = VisitReplace((BoundCaseExpression &)expr, expression);
		break;
	case ExpressionClass::BOUND_CAST:
		result = VisitReplace((BoundCastExpression &)expr, expression);
		break;
	case ExpressionClass::BOUND_COLUMN_REF:
		result = VisitReplace((BoundColumnRefExpression &)expr, expression);
		break;
	case ExpressionClass::BOUND_COMPARISON:
		result = VisitReplace((BoundComparisonExpression &)expr, expression);
		break;
	case ExpressionClass::BOUND_CONJUNCTION:
		result = VisitReplace((BoundConjunctionExpression &)expr, expression);
		break;
	case ExpressionClass::BOUND_CONSTANT:
		result = VisitReplace((BoundConstantExpression &)expr, expression);
		break;
	case ExpressionClass::BOUND_FILTER:
		result = VisitReplace((BoundFilterExpression &)expr, expression);
		break;
	case ExpressionClass::BOUND_FUNCTION:
		result = VisitReplace((BoundFunctionExpression &)expr, expression);
		break;
	case ExpressionClass::BOUND_SUBQUERY:
		result = VisitReplace((BoundSubqueryExpression &)expr, expression);
		break;
	case ExpressionClass::BOUND_OPERATOR:
		result = VisitReplace((BoundOperatorExpression &)expr, expression);
		break;
	case ExpressionClass::BOUND_PARAMETER:
		result = VisitReplace((BoundParameterExpression &)expr, expression);
		break;
	case ExpressionClass::BOUND_REF:
		result = VisitReplace((BoundReferenceExpression &)expr, expression);
		break;
	case ExpressionClass::BOUND_DEFAULT:
		result = VisitReplace((BoundDefaultExpression &)expr, expression);
		break;
	case ExpressionClass::BOUND_WINDOW:
		result = VisitReplace((BoundWindowExpression &)expr, expression);
		break;
	case ExpressionClass::BOUND_UNNEST:
		result = VisitReplace((BoundUnnestExpression &)expr, expression);
		break;
	default:
		D_ASSERT(0);
	}
	if (result) {
		*expression = move(result);
	} else {
		// visit the children of this node
		VisitExpressionChildren(expr);
	}
}

void LogicalOperatorVisitor::VisitExpressionChildren(Expression &expr) {
	ExpressionIterator::EnumerateChildren(expr, [&](unique_ptr<Expression> &expr) { VisitExpression(&expr); });
}

unique_ptr<Expression> LogicalOperatorVisitor::VisitReplace(BoundAggregateExpression &expr,
                                                            unique_ptr<Expression> *expr_ptr) {
	return nullptr;
}

unique_ptr<Expression> LogicalOperatorVisitor::VisitReplace(BoundBetweenExpression &expr,
                                                            unique_ptr<Expression> *expr_ptr) {
	return nullptr;
}

unique_ptr<Expression> LogicalOperatorVisitor::VisitReplace(BoundCaseExpression &expr,
                                                            unique_ptr<Expression> *expr_ptr) {
	return nullptr;
}

unique_ptr<Expression> LogicalOperatorVisitor::VisitReplace(BoundCastExpression &expr,
                                                            unique_ptr<Expression> *expr_ptr) {
	return nullptr;
}

unique_ptr<Expression> LogicalOperatorVisitor::VisitReplace(BoundColumnRefExpression &expr,
                                                            unique_ptr<Expression> *expr_ptr) {
	return nullptr;
}

unique_ptr<Expression> LogicalOperatorVisitor::VisitReplace(BoundComparisonExpression &expr,
                                                            unique_ptr<Expression> *expr_ptr) {
	return nullptr;
}

unique_ptr<Expression> LogicalOperatorVisitor::VisitReplace(BoundConjunctionExpression &expr,
                                                            unique_ptr<Expression> *expr_ptr) {
	return nullptr;
}

unique_ptr<Expression> LogicalOperatorVisitor::VisitReplace(BoundConstantExpression &expr,
                                                            unique_ptr<Expression> *expr_ptr) {
	return nullptr;
}

unique_ptr<Expression> LogicalOperatorVisitor::VisitReplace(BoundDefaultExpression &expr,
                                                            unique_ptr<Expression> *expr_ptr) {
	return nullptr;
}

unique_ptr<Expression> LogicalOperatorVisitor::VisitReplace(BoundFilterExpression &expr,
                                                            unique_ptr<Expression> *expr_ptr) {
	return nullptr;
}

unique_ptr<Expression> LogicalOperatorVisitor::VisitReplace(BoundFunctionExpression &expr,
                                                            unique_ptr<Expression> *expr_ptr) {
	return nullptr;
}

unique_ptr<Expression> LogicalOperatorVisitor::VisitReplace(BoundOperatorExpression &expr,
                                                            unique_ptr<Expression> *expr_ptr) {
	return nullptr;
}

unique_ptr<Expression> LogicalOperatorVisitor::VisitReplace(BoundParameterExpression &expr,
                                                            unique_ptr<Expression> *expr_ptr) {
	return nullptr;
}

unique_ptr<Expression> LogicalOperatorVisitor::VisitReplace(BoundReferenceExpression &expr,
                                                            unique_ptr<Expression> *expr_ptr) {
	return nullptr;
}

unique_ptr<Expression> LogicalOperatorVisitor::VisitReplace(BoundSubqueryExpression &expr,
                                                            unique_ptr<Expression> *expr_ptr) {
	return nullptr;
}

unique_ptr<Expression> LogicalOperatorVisitor::VisitReplace(BoundWindowExpression &expr,
                                                            unique_ptr<Expression> *expr_ptr) {
	return nullptr;
}

unique_ptr<Expression> LogicalOperatorVisitor::VisitReplace(BoundUnnestExpression &expr,
                                                            unique_ptr<Expression> *expr_ptr) {
	return nullptr;
}

} // namespace duckdb
