#include "duckdb/parser/parsed_expression_iterator.hpp"

#include "duckdb/parser/expression/list.hpp"
#include "duckdb/parser/query_node.hpp"
#include "duckdb/parser/query_node/recursive_cte_node.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/query_node/set_operation_node.hpp"
#include "duckdb/parser/tableref/list.hpp"

namespace duckdb {

void ParsedExpressionIterator::EnumerateChildren(const ParsedExpression &expression,
                                                 const std::function<void(const ParsedExpression &child)> &callback) {
	EnumerateChildren((ParsedExpression &)expression, [&](unique_ptr<ParsedExpression> &child) {
		D_ASSERT(child);
		callback(*child);
	});
}

void ParsedExpressionIterator::EnumerateChildren(ParsedExpression &expr,
                                                 const std::function<void(ParsedExpression &child)> &callback) {
	EnumerateChildren(expr, [&](unique_ptr<ParsedExpression> &child) {
		D_ASSERT(child);
		callback(*child);
	});
}

void ParsedExpressionIterator::EnumerateChildren(
    ParsedExpression &expr, const std::function<void(unique_ptr<ParsedExpression> &child)> &callback) {
	switch (expr.expression_class) {
	case ExpressionClass::BETWEEN: {
		auto &cast_expr = (BetweenExpression &)expr;
		callback(cast_expr.input);
		callback(cast_expr.lower);
		callback(cast_expr.upper);
		break;
	}
	case ExpressionClass::CASE: {
		auto &case_expr = (CaseExpression &)expr;
		for (auto &check : case_expr.case_checks) {
			callback(check.when_expr);
			callback(check.then_expr);
		}
		callback(case_expr.else_expr);
		break;
	}
	case ExpressionClass::CAST: {
		auto &cast_expr = (CastExpression &)expr;
		callback(cast_expr.child);
		break;
	}
	case ExpressionClass::COLLATE: {
		auto &cast_expr = (CollateExpression &)expr;
		callback(cast_expr.child);
		break;
	}
	case ExpressionClass::COMPARISON: {
		auto &comp_expr = (ComparisonExpression &)expr;
		callback(comp_expr.left);
		callback(comp_expr.right);
		break;
	}
	case ExpressionClass::CONJUNCTION: {
		auto &conj_expr = (ConjunctionExpression &)expr;
		for (auto &child : conj_expr.children) {
			callback(child);
		}
		break;
	}

	case ExpressionClass::FUNCTION: {
		auto &func_expr = (FunctionExpression &)expr;
		for (auto &child : func_expr.children) {
			callback(child);
		}
		if (func_expr.filter) {
			callback(func_expr.filter);
		}
		if (func_expr.order_bys) {
			for (auto &order : func_expr.order_bys->orders) {
				callback(order.expression);
			}
		}
		break;
	}
	case ExpressionClass::LAMBDA: {
		auto &lambda_expr = (LambdaExpression &)expr;
		callback(lambda_expr.lhs);
		callback(lambda_expr.expr);
		break;
	}
	case ExpressionClass::OPERATOR: {
		auto &op_expr = (OperatorExpression &)expr;
		for (auto &child : op_expr.children) {
			callback(child);
		}
		break;
	}
	case ExpressionClass::SUBQUERY: {
		auto &subquery_expr = (SubqueryExpression &)expr;
		if (subquery_expr.child) {
			callback(subquery_expr.child);
		}
		break;
	}
	case ExpressionClass::WINDOW: {
		auto &window_expr = (WindowExpression &)expr;
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
		break;
	}
	case ExpressionClass::BOUND_EXPRESSION:
	case ExpressionClass::COLUMN_REF:
	case ExpressionClass::CONSTANT:
	case ExpressionClass::DEFAULT:
	case ExpressionClass::STAR:
	case ExpressionClass::PARAMETER:
	case ExpressionClass::POSITIONAL_REFERENCE:
		// these node types have no children
		break;
	default:
		// called on non ParsedExpression type!
		throw NotImplementedException("Unimplemented expression class");
	}
}

void ParsedExpressionIterator::EnumerateQueryNodeModifiers(
    QueryNode &node, const std::function<void(unique_ptr<ParsedExpression> &child)> &callback) {

	for (auto &modifier : node.modifiers) {
		switch (modifier->type) {
		case ResultModifierType::LIMIT_MODIFIER: {
			auto &limit_modifier = (LimitModifier &)*modifier;
			if (limit_modifier.limit) {
				callback(limit_modifier.limit);
			}
			if (limit_modifier.offset) {
				callback(limit_modifier.offset);
			}
		} break;

		case ResultModifierType::LIMIT_PERCENT_MODIFIER: {
			auto &limit_modifier = (LimitPercentModifier &)*modifier;
			if (limit_modifier.limit) {
				callback(limit_modifier.limit);
			}
			if (limit_modifier.offset) {
				callback(limit_modifier.offset);
			}
		} break;

		case ResultModifierType::ORDER_MODIFIER: {
			auto &order_modifier = (OrderModifier &)*modifier;
			for (auto &order : order_modifier.orders) {
				callback(order.expression);
			}
		} break;

		case ResultModifierType::DISTINCT_MODIFIER: {
			auto &distinct_modifier = (DistinctModifier &)*modifier;
			for (auto &target : distinct_modifier.distinct_on_targets) {
				callback(target);
			}
		} break;

		// do nothing
		default:
			break;
		}
	}
}

void ParsedExpressionIterator::EnumerateTableRefChildren(
    TableRef &ref, const std::function<void(unique_ptr<ParsedExpression> &child)> &callback) {
	switch (ref.type) {
	case TableReferenceType::CROSS_PRODUCT: {
		auto &cp_ref = (CrossProductRef &)ref;
		EnumerateTableRefChildren(*cp_ref.left, callback);
		EnumerateTableRefChildren(*cp_ref.right, callback);
		break;
	}
	case TableReferenceType::EXPRESSION_LIST: {
		auto &el_ref = (ExpressionListRef &)ref;
		for (idx_t i = 0; i < el_ref.values.size(); i++) {
			for (idx_t j = 0; j < el_ref.values[i].size(); j++) {
				callback(el_ref.values[i][j]);
			}
		}
		break;
	}
	case TableReferenceType::JOIN: {
		auto &j_ref = (JoinRef &)ref;
		EnumerateTableRefChildren(*j_ref.left, callback);
		EnumerateTableRefChildren(*j_ref.right, callback);
		if (j_ref.condition) {
			callback(j_ref.condition);
		}
		break;
	}
	case TableReferenceType::SUBQUERY: {
		auto &sq_ref = (SubqueryRef &)ref;
		EnumerateQueryNodeChildren(*sq_ref.subquery->node, callback);
		break;
	}
	case TableReferenceType::TABLE_FUNCTION: {
		auto &tf_ref = (TableFunctionRef &)ref;
		callback(tf_ref.function);
		break;
	}
	case TableReferenceType::BASE_TABLE:
	case TableReferenceType::EMPTY:
		// these TableRefs do not need to be unfolded
		break;
	default:
		throw NotImplementedException("TableRef type not implemented for traversal");
	}
}

void ParsedExpressionIterator::EnumerateQueryNodeChildren(
    QueryNode &node, const std::function<void(unique_ptr<ParsedExpression> &child)> &callback) {
	switch (node.type) {
	case QueryNodeType::RECURSIVE_CTE_NODE: {
		auto &rcte_node = (RecursiveCTENode &)node;
		EnumerateQueryNodeChildren(*rcte_node.left, callback);
		EnumerateQueryNodeChildren(*rcte_node.right, callback);
		break;
	}
	case QueryNodeType::SELECT_NODE: {
		auto &sel_node = (SelectNode &)node;
		for (idx_t i = 0; i < sel_node.select_list.size(); i++) {
			callback(sel_node.select_list[i]);
		}
		for (idx_t i = 0; i < sel_node.groups.group_expressions.size(); i++) {
			callback(sel_node.groups.group_expressions[i]);
		}
		if (sel_node.where_clause) {
			callback(sel_node.where_clause);
		}
		if (sel_node.having) {
			callback(sel_node.having);
		}
		if (sel_node.qualify) {
			callback(sel_node.qualify);
		}

		EnumerateTableRefChildren(*sel_node.from_table.get(), callback);
		break;
	}
	case QueryNodeType::SET_OPERATION_NODE: {
		auto &setop_node = (SetOperationNode &)node;
		EnumerateQueryNodeChildren(*setop_node.left, callback);
		EnumerateQueryNodeChildren(*setop_node.right, callback);
		break;
	}
	default:
		throw NotImplementedException("QueryNode type not implemented for traversal");
	}

	if (!node.modifiers.empty()) {
		EnumerateQueryNodeModifiers(node, callback);
	}

	for (auto &kv : node.cte_map.map) {
		EnumerateQueryNodeChildren(*kv.second->query->node, callback);
	}
}

} // namespace duckdb
