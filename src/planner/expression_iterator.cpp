#include "duckdb/planner/expression_iterator.hpp"

#include "duckdb/planner/bound_query_node.hpp"
#include "duckdb/planner/expression/list.hpp"
#include "duckdb/planner/query_node/bound_select_node.hpp"
#include "duckdb/planner/query_node/bound_set_operation_node.hpp"
#include "duckdb/planner/query_node/bound_recursive_cte_node.hpp"
#include "duckdb/planner/query_node/bound_cte_node.hpp"
#include "duckdb/planner/tableref/list.hpp"
#include "duckdb/common/enum_util.hpp"

namespace duckdb {

void ExpressionIterator::EnumerateChildren(const Expression &expr,
                                           const std::function<void(const Expression &child)> &callback) {
	EnumerateChildren((Expression &)expr, [&](unique_ptr<Expression> &child) { callback(*child); });
}

void ExpressionIterator::EnumerateChildren(Expression &expr, const std::function<void(Expression &child)> &callback) {
	EnumerateChildren(expr, [&](unique_ptr<Expression> &child) { callback(*child); });
}

void ExpressionIterator::EnumerateChildren(Expression &expr,
                                           const std::function<void(unique_ptr<Expression> &child)> &callback) {
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::BOUND_AGGREGATE: {
		auto &aggr_expr = expr.Cast<BoundAggregateExpression>();
		for (auto &child : aggr_expr.children) {
			callback(child);
		}
		if (aggr_expr.filter) {
			callback(aggr_expr.filter);
		}
		if (aggr_expr.order_bys) {
			for (auto &order : aggr_expr.order_bys->orders) {
				callback(order.expression);
			}
		}
		break;
	}
	case ExpressionClass::BOUND_BETWEEN: {
		auto &between_expr = expr.Cast<BoundBetweenExpression>();
		callback(between_expr.input);
		callback(between_expr.lower);
		callback(between_expr.upper);
		break;
	}
	case ExpressionClass::BOUND_CASE: {
		auto &case_expr = expr.Cast<BoundCaseExpression>();
		for (auto &case_check : case_expr.case_checks) {
			callback(case_check.when_expr);
			callback(case_check.then_expr);
		}
		callback(case_expr.else_expr);
		break;
	}
	case ExpressionClass::BOUND_CAST: {
		auto &cast_expr = expr.Cast<BoundCastExpression>();
		callback(cast_expr.child);
		break;
	}
	case ExpressionClass::BOUND_COMPARISON: {
		auto &comp_expr = expr.Cast<BoundComparisonExpression>();
		callback(comp_expr.left);
		callback(comp_expr.right);
		break;
	}
	case ExpressionClass::BOUND_CONJUNCTION: {
		auto &conj_expr = expr.Cast<BoundConjunctionExpression>();
		for (auto &child : conj_expr.children) {
			callback(child);
		}
		break;
	}
	case ExpressionClass::BOUND_FUNCTION: {
		auto &func_expr = expr.Cast<BoundFunctionExpression>();
		for (auto &child : func_expr.children) {
			callback(child);
		}
		break;
	}
	case ExpressionClass::BOUND_OPERATOR: {
		auto &op_expr = expr.Cast<BoundOperatorExpression>();
		for (auto &child : op_expr.children) {
			callback(child);
		}
		break;
	}
	case ExpressionClass::BOUND_SUBQUERY: {
		auto &subquery_expr = expr.Cast<BoundSubqueryExpression>();
		for (auto &child : subquery_expr.children) {
			callback(child);
		}
		break;
	}
	case ExpressionClass::BOUND_WINDOW: {
		auto &window_expr = expr.Cast<BoundWindowExpression>();
		for (auto &partition : window_expr.partitions) {
			callback(partition);
		}
		for (auto &order : window_expr.orders) {
			callback(order.expression);
		}
		for (auto &child : window_expr.children) {
			callback(child);
		}
		if (window_expr.filter_expr) {
			callback(window_expr.filter_expr);
		}
		if (window_expr.start_expr) {
			callback(window_expr.start_expr);
		}
		if (window_expr.end_expr) {
			callback(window_expr.end_expr);
		}
		if (window_expr.offset_expr) {
			callback(window_expr.offset_expr);
		}
		if (window_expr.default_expr) {
			callback(window_expr.default_expr);
		}
		for (auto &order : window_expr.arg_orders) {
			callback(order.expression);
		}
		break;
	}
	case ExpressionClass::BOUND_UNNEST: {
		auto &unnest_expr = expr.Cast<BoundUnnestExpression>();
		callback(unnest_expr.child);
		break;
	}
	case ExpressionClass::BOUND_COLUMN_REF:
	case ExpressionClass::BOUND_LAMBDA_REF:
	case ExpressionClass::BOUND_CONSTANT:
	case ExpressionClass::BOUND_DEFAULT:
	case ExpressionClass::BOUND_PARAMETER:
	case ExpressionClass::BOUND_REF:
		// these node types have no children
		break;
	default:
		throw InternalException("ExpressionIterator used on unbound expression");
	}
}

void ExpressionIterator::EnumerateExpression(unique_ptr<Expression> &expr,
                                             const std::function<void(Expression &child)> &callback) {
	if (!expr) {
		return;
	}
	callback(*expr);
	ExpressionIterator::EnumerateChildren(*expr,
	                                      [&](unique_ptr<Expression> &child) { EnumerateExpression(child, callback); });
}

void BoundNodeVisitor::VisitExpression(unique_ptr<Expression> &expression) {
	VisitExpressionChildren(*expression);
}

void BoundNodeVisitor::VisitExpressionChildren(Expression &expr) {
	ExpressionIterator::EnumerateChildren(expr, [&](unique_ptr<Expression> &expr) { VisitExpression(expr); });
}

void BoundNodeVisitor::VisitBoundQueryNode(BoundQueryNode &node) {
	switch (node.type) {
	case QueryNodeType::SET_OPERATION_NODE: {
		auto &bound_setop = node.Cast<BoundSetOperationNode>();
		VisitBoundQueryNode(*bound_setop.left);
		VisitBoundQueryNode(*bound_setop.right);
		break;
	}
	case QueryNodeType::RECURSIVE_CTE_NODE: {
		auto &cte_node = node.Cast<BoundRecursiveCTENode>();
		VisitBoundQueryNode(*cte_node.left);
		VisitBoundQueryNode(*cte_node.right);
		break;
	}
	case QueryNodeType::CTE_NODE: {
		auto &cte_node = node.Cast<BoundCTENode>();
		VisitBoundQueryNode(*cte_node.child);
		VisitBoundQueryNode(*cte_node.query);
		break;
	}
	case QueryNodeType::SELECT_NODE: {
		auto &bound_select = node.Cast<BoundSelectNode>();
		for (auto &expr : bound_select.select_list) {
			VisitExpression(expr);
		}
		if (bound_select.where_clause) {
			VisitExpression(bound_select.where_clause);
		}
		for (auto &expr : bound_select.groups.group_expressions) {
			VisitExpression(expr);
		}
		if (bound_select.having) {
			VisitExpression(bound_select.having);
		}
		for (auto &expr : bound_select.aggregates) {
			VisitExpression(expr);
		}
		for (auto &entry : bound_select.unnests) {
			for (auto &expr : entry.second.expressions) {
				VisitExpression(expr);
			}
		}
		for (auto &expr : bound_select.windows) {
			VisitExpression(expr);
		}
		if (bound_select.from_table) {
			VisitBoundTableRef(*bound_select.from_table);
		}
		break;
	}
	default:
		throw NotImplementedException("Unimplemented query node in ExpressionIterator");
	}
	for (idx_t i = 0; i < node.modifiers.size(); i++) {
		switch (node.modifiers[i]->type) {
		case ResultModifierType::DISTINCT_MODIFIER:
			for (auto &expr : node.modifiers[i]->Cast<BoundDistinctModifier>().target_distincts) {
				VisitExpression(expr);
			}
			break;
		case ResultModifierType::ORDER_MODIFIER:
			for (auto &order : node.modifiers[i]->Cast<BoundOrderModifier>().orders) {
				VisitExpression(order.expression);
			}
			break;
		case ResultModifierType::LIMIT_MODIFIER: {
			auto &limit_expr = node.modifiers[i]->Cast<BoundLimitModifier>().limit_val.GetExpression();
			auto &offset_expr = node.modifiers[i]->Cast<BoundLimitModifier>().offset_val.GetExpression();
			if (limit_expr) {
				VisitExpression(limit_expr);
			}
			if (offset_expr) {
				VisitExpression(offset_expr);
			}
			break;
		}
		default:
			break;
		}
	}
}

class LogicalBoundNodeVisitor : public LogicalOperatorVisitor {
public:
	explicit LogicalBoundNodeVisitor(BoundNodeVisitor &parent) : parent(parent) {
	}

	void VisitExpression(unique_ptr<Expression> *expression) override {
		auto &expr = **expression;
		parent.VisitExpression(*expression);
		VisitExpressionChildren(expr);
	}

protected:
	BoundNodeVisitor &parent;
};

void BoundNodeVisitor::VisitBoundTableRef(BoundTableRef &ref) {
	switch (ref.type) {
	case TableReferenceType::EXPRESSION_LIST: {
		auto &bound_expr_list = ref.Cast<BoundExpressionListRef>();
		for (auto &expr_list : bound_expr_list.values) {
			for (auto &expr : expr_list) {
				VisitExpression(expr);
			}
		}
		break;
	}
	case TableReferenceType::JOIN: {
		auto &bound_join = ref.Cast<BoundJoinRef>();
		if (bound_join.condition) {
			VisitExpression(bound_join.condition);
		}
		VisitBoundTableRef(*bound_join.left);
		VisitBoundTableRef(*bound_join.right);
		break;
	}
	case TableReferenceType::SUBQUERY: {
		auto &bound_subquery = ref.Cast<BoundSubqueryRef>();
		VisitBoundQueryNode(*bound_subquery.subquery);
		break;
	}
	case TableReferenceType::TABLE_FUNCTION: {
		auto &bound_table_function = ref.Cast<BoundTableFunction>();
		LogicalBoundNodeVisitor node_visitor(*this);
		if (bound_table_function.get) {
			node_visitor.VisitOperator(*bound_table_function.get);
		}
		if (bound_table_function.subquery) {
			VisitBoundTableRef(*bound_table_function.subquery);
		}
		break;
	}
	case TableReferenceType::EMPTY_FROM:
	case TableReferenceType::BASE_TABLE:
	case TableReferenceType::CTE:
		break;
	default:
		throw NotImplementedException("Unimplemented table reference type (%s) in ExpressionIterator",
		                              EnumUtil::ToString(ref.type));
	}
}

} // namespace duckdb
