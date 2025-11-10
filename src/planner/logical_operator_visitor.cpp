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
	if (op.HasProjectionMap()) {
		VisitOperatorWithProjectionMapChildren(op);
	} else {
		for (auto &child : op.children) {
			VisitOperator(*child);
		}
	}
}

void LogicalOperatorVisitor::VisitOperatorWithProjectionMapChildren(LogicalOperator &op) {
	D_ASSERT(op.HasProjectionMap());
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_ANY_JOIN:
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
	case LogicalOperatorType::LOGICAL_DELIM_JOIN:
	case LogicalOperatorType::LOGICAL_ASOF_JOIN: {
		auto &join = op.Cast<LogicalJoin>();
		VisitChildOfOperatorWithProjectionMap(*op.children[0], join.left_projection_map);
		VisitChildOfOperatorWithProjectionMap(*op.children[1], join.right_projection_map);
		break;
	}
	case LogicalOperatorType::LOGICAL_ORDER_BY: {
		auto &order = op.Cast<LogicalOrder>();
		VisitChildOfOperatorWithProjectionMap(*op.children[0], order.projection_map);
		break;
	}
	case LogicalOperatorType::LOGICAL_FILTER: {
		auto &filter = op.Cast<LogicalFilter>();
		VisitChildOfOperatorWithProjectionMap(*op.children[0], filter.projection_map);
		break;
	}
	default:
		throw NotImplementedException("VisitOperatorWithProjectionMapChildren for %s", EnumUtil::ToString(op.type));
	}
}

void LogicalOperatorVisitor::VisitChildOfOperatorWithProjectionMap(LogicalOperator &child,
                                                                   vector<idx_t> &projection_map) {
	const auto child_bindings_before = child.GetColumnBindings();
	VisitOperator(child);
	if (projection_map.empty()) {
		return; // Nothing to fix here
	}
	// Child binding order may have changed due to 'fun'.
	const auto child_bindings_after = child.GetColumnBindings();
	if (child_bindings_before == child_bindings_after) {
		return; // Nothing changed
	}
	// The desired order is 'projection_map' applied to 'child_bindings_before'
	// We create 'new_projection_map', which ensures this order even if 'child_bindings_after' is different
	vector<idx_t> new_projection_map;
	new_projection_map.reserve(projection_map.size());
	for (const auto proj_idx_before : projection_map) {
		auto &desired_binding = child_bindings_before[proj_idx_before];
		idx_t proj_idx_after;
		for (proj_idx_after = 0; proj_idx_after < child_bindings_after.size(); proj_idx_after++) {
			if (child_bindings_after[proj_idx_after] == desired_binding) {
				break;
			}
		}
		if (proj_idx_after == child_bindings_after.size()) {
			// VisitOperator has removed this binding, e.g., by replacing one binding with another
			// Inside here we don't know how it has been replaced, and projection maps are positional: bail
			new_projection_map.clear();
			break;
		}
		new_projection_map.push_back(proj_idx_after);
	}
	projection_map = std::move(new_projection_map);
}

void LogicalOperatorVisitor::EnumerateExpressions(LogicalOperator &op,
                                                  const std::function<void(unique_ptr<Expression> *child)> &callback) {
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_EXPRESSION_GET: {
		auto &get = op.Cast<LogicalExpressionGet>();
		for (auto &expr_list : get.expressions) {
			for (auto &expr : expr_list) {
				callback(&expr);
			}
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_ORDER_BY: {
		auto &order = op.Cast<LogicalOrder>();
		for (auto &node : order.orders) {
			callback(&node.expression);
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_TOP_N: {
		auto &order = op.Cast<LogicalTopN>();
		for (auto &node : order.orders) {
			callback(&node.expression);
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_DISTINCT: {
		auto &distinct = op.Cast<LogicalDistinct>();
		for (auto &target : distinct.distinct_targets) {
			callback(&target);
		}
		if (distinct.order_by) {
			for (auto &order : distinct.order_by->orders) {
				callback(&order.expression);
			}
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_RECURSIVE_CTE: {
		auto &rec = op.Cast<LogicalRecursiveCTE>();

		for (auto &target : rec.key_targets) {
			callback(&target);
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_INSERT: {
		auto &insert = op.Cast<LogicalInsert>();
		if (insert.on_conflict_info.on_conflict_condition) {
			callback(&insert.on_conflict_info.on_conflict_condition);
		}
		if (insert.on_conflict_info.do_update_condition) {
			callback(&insert.on_conflict_info.do_update_condition);
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_DEPENDENT_JOIN: {
		auto &join = op.Cast<LogicalDependentJoin>();
		for (auto &expr : join.duplicate_eliminated_columns) {
			callback(&expr);
		}
		for (auto &cond : join.conditions) {
			callback(&cond.left);
			callback(&cond.right);
		}
		for (auto &expr : join.arbitrary_expressions) {
			callback(&expr);
		}
		for (auto &expr : join.expression_children) {
			callback(&expr);
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_ASOF_JOIN:
	case LogicalOperatorType::LOGICAL_DELIM_JOIN:
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN: {
		auto &join = op.Cast<LogicalComparisonJoin>();
		for (auto &expr : join.duplicate_eliminated_columns) {
			callback(&expr);
		}
		for (auto &cond : join.conditions) {
			callback(&cond.left);
			callback(&cond.right);
		}
		if (join.predicate) {
			callback(&join.predicate);
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_ANY_JOIN: {
		auto &join = op.Cast<LogicalAnyJoin>();
		callback(&join.condition);
		break;
	}
	case LogicalOperatorType::LOGICAL_LIMIT: {
		auto &limit = op.Cast<LogicalLimit>();
		if (limit.limit_val.GetExpression()) {
			callback(&limit.limit_val.GetExpression());
		}
		if (limit.offset_val.GetExpression()) {
			callback(&limit.offset_val.GetExpression());
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
		auto &aggr = op.Cast<LogicalAggregate>();
		for (auto &group : aggr.groups) {
			callback(&group);
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_MERGE_INTO: {
		auto &merge_into = op.Cast<LogicalMergeInto>();
		for (auto &entry : merge_into.actions) {
			for (auto &action : entry.second) {
				if (action->condition) {
					callback(&action->condition);
				}
				for (auto &expr : action->expressions) {
					callback(&expr);
				}
			}
		}
		break;
	}
	default:
		break;
	}
	for (auto &expression : op.expressions) {
		callback(&expression);
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
		result = VisitReplace(expr.Cast<BoundAggregateExpression>(), expression);
		break;
	case ExpressionClass::BOUND_BETWEEN:
		result = VisitReplace(expr.Cast<BoundBetweenExpression>(), expression);
		break;
	case ExpressionClass::BOUND_CASE:
		result = VisitReplace(expr.Cast<BoundCaseExpression>(), expression);
		break;
	case ExpressionClass::BOUND_CAST:
		result = VisitReplace(expr.Cast<BoundCastExpression>(), expression);
		break;
	case ExpressionClass::BOUND_COLUMN_REF:
		result = VisitReplace(expr.Cast<BoundColumnRefExpression>(), expression);
		break;
	case ExpressionClass::BOUND_COMPARISON:
		result = VisitReplace(expr.Cast<BoundComparisonExpression>(), expression);
		break;
	case ExpressionClass::BOUND_CONJUNCTION:
		result = VisitReplace(expr.Cast<BoundConjunctionExpression>(), expression);
		break;
	case ExpressionClass::BOUND_CONSTANT:
		result = VisitReplace(expr.Cast<BoundConstantExpression>(), expression);
		break;
	case ExpressionClass::BOUND_FUNCTION:
		result = VisitReplace(expr.Cast<BoundFunctionExpression>(), expression);
		break;
	case ExpressionClass::BOUND_SUBQUERY:
		result = VisitReplace(expr.Cast<BoundSubqueryExpression>(), expression);
		break;
	case ExpressionClass::BOUND_OPERATOR:
		result = VisitReplace(expr.Cast<BoundOperatorExpression>(), expression);
		break;
	case ExpressionClass::BOUND_PARAMETER:
		result = VisitReplace(expr.Cast<BoundParameterExpression>(), expression);
		break;
	case ExpressionClass::BOUND_REF:
		result = VisitReplace(expr.Cast<BoundReferenceExpression>(), expression);
		break;
	case ExpressionClass::BOUND_DEFAULT:
		result = VisitReplace(expr.Cast<BoundDefaultExpression>(), expression);
		break;
	case ExpressionClass::BOUND_WINDOW:
		result = VisitReplace(expr.Cast<BoundWindowExpression>(), expression);
		break;
	case ExpressionClass::BOUND_UNNEST:
		result = VisitReplace(expr.Cast<BoundUnnestExpression>(), expression);
		break;
	default:
		throw InternalException("Unrecognized expression type in logical operator visitor");
	}
	if (result) {
		*expression = std::move(result);
	} else {
		// visit the children of this node
		VisitExpressionChildren(expr);
	}
}

void LogicalOperatorVisitor::VisitExpressionChildren(Expression &expr) {
	ExpressionIterator::EnumerateChildren(expr, [&](unique_ptr<Expression> &expr) { VisitExpression(&expr); });
}

// these are all default methods that can be overriden
// we don't care about coverage here
// LCOV_EXCL_START
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

// LCOV_EXCL_STOP

} // namespace duckdb
