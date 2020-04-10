#include "duckdb/planner/expression_iterator.hpp"

#include "duckdb/planner/bound_query_node.hpp"
#include "duckdb/planner/expression/list.hpp"
#include "duckdb/planner/query_node/bound_select_node.hpp"
#include "duckdb/planner/query_node/bound_set_operation_node.hpp"
#include "duckdb/planner/tableref/list.hpp"

using namespace duckdb;
using namespace std;

void ExpressionIterator::EnumerateChildren(const Expression &expr, function<void(const Expression &child)> callback) {
	EnumerateChildren((Expression &)expr, [&](unique_ptr<Expression> child) -> unique_ptr<Expression> {
		callback(*child);
		return move(child);
	});
}

void ExpressionIterator::EnumerateChildren(Expression &expr, std::function<void(Expression &child)> callback) {
	EnumerateChildren(expr, [&](unique_ptr<Expression> child) -> unique_ptr<Expression> {
		callback(*child);
		return move(child);
	});
}

void ExpressionIterator::EnumerateChildren(Expression &expr,
                                           function<unique_ptr<Expression>(unique_ptr<Expression> child)> callback) {
	switch (expr.expression_class) {
	case ExpressionClass::BOUND_AGGREGATE: {
		auto &aggr_expr = (BoundAggregateExpression &)expr;
		for (auto &child : aggr_expr.children) {
			child = callback(move(child));
		}
		break;
	}
	case ExpressionClass::BOUND_BETWEEN: {
		auto &between_expr = (BoundBetweenExpression &)expr;
		between_expr.input = callback(move(between_expr.input));
		between_expr.lower = callback(move(between_expr.lower));
		between_expr.upper = callback(move(between_expr.upper));
		break;
	}
	case ExpressionClass::BOUND_CASE: {
		auto &case_expr = (BoundCaseExpression &)expr;
		case_expr.check = callback(move(case_expr.check));
		case_expr.result_if_true = callback(move(case_expr.result_if_true));
		case_expr.result_if_false = callback(move(case_expr.result_if_false));
		break;
	}
	case ExpressionClass::BOUND_CAST: {
		auto &cast_expr = (BoundCastExpression &)expr;
		cast_expr.child = callback(move(cast_expr.child));
		break;
	}
	case ExpressionClass::BOUND_COMPARISON: {
		auto &comp_expr = (BoundComparisonExpression &)expr;
		comp_expr.left = callback(move(comp_expr.left));
		comp_expr.right = callback(move(comp_expr.right));
		break;
	}
	case ExpressionClass::BOUND_CONJUNCTION: {
		auto &conj_expr = (BoundConjunctionExpression &)expr;
		for (auto &child : conj_expr.children) {
			child = callback(move(child));
		}
		break;
	}
	case ExpressionClass::BOUND_FUNCTION: {
		auto &func_expr = (BoundFunctionExpression &)expr;
		for (auto &child : func_expr.children) {
			child = callback(move(child));
		}
		break;
	}
	case ExpressionClass::BOUND_OPERATOR: {
		auto &op_expr = (BoundOperatorExpression &)expr;
		for (auto &child : op_expr.children) {
			child = callback(move(child));
		}
		break;
	}
	case ExpressionClass::BOUND_SUBQUERY: {
		auto &subquery_expr = (BoundSubqueryExpression &)expr;
		if (subquery_expr.child) {
			subquery_expr.child = callback(move(subquery_expr.child));
		}
		break;
	}
	case ExpressionClass::BOUND_WINDOW: {
		auto &window_expr = (BoundWindowExpression &)expr;
		for (auto &partition : window_expr.partitions) {
			partition = callback(move(partition));
		}
		for (auto &order : window_expr.orders) {
			order.expression = callback(move(order.expression));
		}
		for (auto &child : window_expr.children) {
			child = callback(move(child));
		}
		if (window_expr.offset_expr) {
			window_expr.offset_expr = callback(move(window_expr.offset_expr));
		}
		if (window_expr.default_expr) {
			window_expr.default_expr = callback(move(window_expr.default_expr));
		}
		break;
	}
	case ExpressionClass::BOUND_UNNEST: {
		auto &unnest_expr = (BoundUnnestExpression &)expr;
		unnest_expr.child = callback(move(unnest_expr.child));
		break;
	}
	case ExpressionClass::COMMON_SUBEXPRESSION: {
		auto &cse_expr = (CommonSubExpression &)expr;
		if (cse_expr.owned_child) {
			cse_expr.owned_child = callback(move(cse_expr.owned_child));
		}
		break;
	}
	case ExpressionClass::BOUND_COLUMN_REF:
	case ExpressionClass::BOUND_CONSTANT:
	case ExpressionClass::BOUND_DEFAULT:
	case ExpressionClass::BOUND_PARAMETER:
	case ExpressionClass::BOUND_REF:
		// these node types have no children
		break;
	default:
		// called on non BoundExpression type!
		assert(0);
		break;
	}
}

void ExpressionIterator::EnumerateExpression(unique_ptr<Expression> &expr,
                                             std::function<void(Expression &child)> callback) {
	if (!expr) {
		return;
	}
	callback(*expr);
	ExpressionIterator::EnumerateChildren(*expr, [&](unique_ptr<Expression> child) -> unique_ptr<Expression> {
		EnumerateExpression(child, callback);
		return move(child);
	});
}

void ExpressionIterator::EnumerateTableRefChildren(BoundTableRef &ref,
                                                   std::function<void(Expression &child)> callback) {
	switch (ref.type) {
	case TableReferenceType::CROSS_PRODUCT: {
		auto &bound_crossproduct = (BoundCrossProductRef &)ref;
		EnumerateTableRefChildren(*bound_crossproduct.left, callback);
		EnumerateTableRefChildren(*bound_crossproduct.right, callback);
		break;
	}
	case TableReferenceType::JOIN: {
		auto &bound_join = (BoundJoinRef &)ref;
		EnumerateExpression(bound_join.condition, callback);
		EnumerateTableRefChildren(*bound_join.left, callback);
		EnumerateTableRefChildren(*bound_join.right, callback);
		break;
	}
	case TableReferenceType::SUBQUERY: {
		auto &bound_subquery = (BoundSubqueryRef &)ref;
		EnumerateQueryNodeChildren(*bound_subquery.subquery, callback);
		break;
	}
	default:
		assert(ref.type == TableReferenceType::TABLE_FUNCTION || ref.type == TableReferenceType::BASE_TABLE ||
		       ref.type == TableReferenceType::EMPTY);
		break;
	}
}

void ExpressionIterator::EnumerateQueryNodeChildren(BoundQueryNode &node,
                                                    std::function<void(Expression &child)> callback) {
	switch (node.type) {
	case QueryNodeType::SET_OPERATION_NODE: {
		auto &bound_setop = (BoundSetOperationNode &)node;
		EnumerateQueryNodeChildren(*bound_setop.left, callback);
		EnumerateQueryNodeChildren(*bound_setop.right, callback);
		break;
	}
	default:
		assert(node.type == QueryNodeType::SELECT_NODE);
		auto &bound_select = (BoundSelectNode &)node;
		for (idx_t i = 0; i < bound_select.select_list.size(); i++) {
			EnumerateExpression(bound_select.select_list[i], callback);
		}
		EnumerateExpression(bound_select.where_clause, callback);
		for (idx_t i = 0; i < bound_select.groups.size(); i++) {
			EnumerateExpression(bound_select.groups[i], callback);
		}
		EnumerateExpression(bound_select.having, callback);
		for (idx_t i = 0; i < bound_select.aggregates.size(); i++) {
			EnumerateExpression(bound_select.aggregates[i], callback);
		}
		for (idx_t i = 0; i < bound_select.windows.size(); i++) {
			EnumerateExpression(bound_select.windows[i], callback);
		}
		if (bound_select.from_table) {
			EnumerateTableRefChildren(*bound_select.from_table, callback);
		}
		break;
	}
	for (idx_t i = 0; i < node.modifiers.size(); i++) {
		switch (node.modifiers[i]->type) {
		case ResultModifierType::DISTINCT_MODIFIER:
			for (auto &expr : ((BoundDistinctModifier &)*node.modifiers[i]).target_distincts) {
				EnumerateExpression(expr, callback);
			}
			break;
		case ResultModifierType::ORDER_MODIFIER:
			for (auto &order : ((BoundOrderModifier &)*node.modifiers[i]).orders) {
				EnumerateExpression(order.expression, callback);
			}
			break;
		default:
			break;
		}
	}
}
