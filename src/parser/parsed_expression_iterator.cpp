#include "duckdb/parser/parsed_expression_iterator.hpp"

#include "duckdb/parser/expression/list.hpp"
#include "duckdb/parser/query_node.hpp"
#include "duckdb/parser/query_node/cte_node.hpp"
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
		auto &cast_expr = expr.Cast<BetweenExpression>();
		callback(cast_expr.input);
		callback(cast_expr.lower);
		callback(cast_expr.upper);
		break;
	}
	case ExpressionClass::CASE: {
		auto &case_expr = expr.Cast<CaseExpression>();
		for (auto &check : case_expr.case_checks) {
			callback(check.when_expr);
			callback(check.then_expr);
		}
		callback(case_expr.else_expr);
		break;
	}
	case ExpressionClass::CAST: {
		auto &cast_expr = expr.Cast<CastExpression>();
		callback(cast_expr.child);
		break;
	}
	case ExpressionClass::COLLATE: {
		auto &cast_expr = expr.Cast<CollateExpression>();
		callback(cast_expr.child);
		break;
	}
	case ExpressionClass::COMPARISON: {
		auto &comp_expr = expr.Cast<ComparisonExpression>();
		callback(comp_expr.left);
		callback(comp_expr.right);
		break;
	}
	case ExpressionClass::CONJUNCTION: {
		auto &conj_expr = expr.Cast<ConjunctionExpression>();
		for (auto &child : conj_expr.children) {
			callback(child);
		}
		break;
	}

	case ExpressionClass::FUNCTION: {
		auto &func_expr = expr.Cast<FunctionExpression>();
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
		auto &lambda_expr = expr.Cast<LambdaExpression>();
		callback(lambda_expr.lhs);
		callback(lambda_expr.expr);
		break;
	}
	case ExpressionClass::OPERATOR: {
		auto &op_expr = expr.Cast<OperatorExpression>();
		for (auto &child : op_expr.children) {
			callback(child);
		}
		break;
	}
	case ExpressionClass::STAR: {
		auto &star_expr = expr.Cast<StarExpression>();
		if (star_expr.expr) {
			callback(star_expr.expr);
		}
		for (auto &item : star_expr.replace_list) {
			callback(item.second);
		}
		break;
	}
	case ExpressionClass::SUBQUERY: {
		auto &subquery_expr = expr.Cast<SubqueryExpression>();
		if (subquery_expr.child) {
			callback(subquery_expr.child);
		}
		break;
	}
	case ExpressionClass::WINDOW: {
		auto &window_expr = expr.Cast<WindowExpression>();
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
	case ExpressionClass::LAMBDA_REF:
	case ExpressionClass::CONSTANT:
	case ExpressionClass::DEFAULT:
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
			auto &limit_modifier = modifier->Cast<LimitModifier>();
			if (limit_modifier.limit) {
				callback(limit_modifier.limit);
			}
			if (limit_modifier.offset) {
				callback(limit_modifier.offset);
			}
		} break;

		case ResultModifierType::LIMIT_PERCENT_MODIFIER: {
			auto &limit_modifier = modifier->Cast<LimitPercentModifier>();
			if (limit_modifier.limit) {
				callback(limit_modifier.limit);
			}
			if (limit_modifier.offset) {
				callback(limit_modifier.offset);
			}
		} break;

		case ResultModifierType::ORDER_MODIFIER: {
			auto &order_modifier = modifier->Cast<OrderModifier>();
			for (auto &order : order_modifier.orders) {
				callback(order.expression);
			}
		} break;

		case ResultModifierType::DISTINCT_MODIFIER: {
			auto &distinct_modifier = modifier->Cast<DistinctModifier>();
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
    TableRef &ref, const std::function<void(unique_ptr<ParsedExpression> &child)> &expr_callback,
    const std::function<void(TableRef &ref)> &ref_callback) {
	switch (ref.type) {
	case TableReferenceType::EXPRESSION_LIST: {
		auto &el_ref = ref.Cast<ExpressionListRef>();
		for (idx_t i = 0; i < el_ref.values.size(); i++) {
			for (idx_t j = 0; j < el_ref.values[i].size(); j++) {
				expr_callback(el_ref.values[i][j]);
			}
		}
		break;
	}
	case TableReferenceType::JOIN: {
		auto &j_ref = ref.Cast<JoinRef>();
		EnumerateTableRefChildren(*j_ref.left, expr_callback, ref_callback);
		EnumerateTableRefChildren(*j_ref.right, expr_callback, ref_callback);
		if (j_ref.condition) {
			expr_callback(j_ref.condition);
		}
		break;
	}
	case TableReferenceType::PIVOT: {
		auto &p_ref = ref.Cast<PivotRef>();
		EnumerateTableRefChildren(*p_ref.source, expr_callback, ref_callback);
		for (auto &aggr : p_ref.aggregates) {
			expr_callback(aggr);
		}
		break;
	}
	case TableReferenceType::SUBQUERY: {
		auto &sq_ref = ref.Cast<SubqueryRef>();
		EnumerateQueryNodeChildren(*sq_ref.subquery->node, expr_callback, ref_callback);
		break;
	}
	case TableReferenceType::TABLE_FUNCTION: {
		auto &tf_ref = ref.Cast<TableFunctionRef>();
		expr_callback(tf_ref.function);
		break;
	}
	case TableReferenceType::BASE_TABLE:
	case TableReferenceType::EMPTY_FROM:
	case TableReferenceType::SHOW_REF:
	case TableReferenceType::COLUMN_DATA:
	case TableReferenceType::DELIM_GET:
		// these TableRefs do not need to be unfolded
		break;
	case TableReferenceType::INVALID:
	case TableReferenceType::CTE:
		throw NotImplementedException("TableRef type not implemented for traversal");
	}
	ref_callback(ref);
}

void ParsedExpressionIterator::EnumerateQueryNodeChildren(
    QueryNode &node, const std::function<void(unique_ptr<ParsedExpression> &child)> &expr_callback,
    const std::function<void(TableRef &ref)> &ref_callback) {
	switch (node.type) {
	case QueryNodeType::RECURSIVE_CTE_NODE: {
		auto &rcte_node = node.Cast<RecursiveCTENode>();
		EnumerateQueryNodeChildren(*rcte_node.left, expr_callback, ref_callback);
		EnumerateQueryNodeChildren(*rcte_node.right, expr_callback, ref_callback);
		break;
	}
	case QueryNodeType::CTE_NODE: {
		auto &cte_node = node.Cast<CTENode>();
		EnumerateQueryNodeChildren(*cte_node.query, expr_callback, ref_callback);
		EnumerateQueryNodeChildren(*cte_node.child, expr_callback, ref_callback);
		break;
	}
	case QueryNodeType::SELECT_NODE: {
		auto &sel_node = node.Cast<SelectNode>();
		for (idx_t i = 0; i < sel_node.select_list.size(); i++) {
			expr_callback(sel_node.select_list[i]);
		}
		for (idx_t i = 0; i < sel_node.groups.group_expressions.size(); i++) {
			expr_callback(sel_node.groups.group_expressions[i]);
		}
		if (sel_node.where_clause) {
			expr_callback(sel_node.where_clause);
		}
		if (sel_node.having) {
			expr_callback(sel_node.having);
		}
		if (sel_node.qualify) {
			expr_callback(sel_node.qualify);
		}

		EnumerateTableRefChildren(*sel_node.from_table.get(), expr_callback, ref_callback);
		break;
	}
	case QueryNodeType::SET_OPERATION_NODE: {
		auto &setop_node = node.Cast<SetOperationNode>();
		EnumerateQueryNodeChildren(*setop_node.left, expr_callback, ref_callback);
		EnumerateQueryNodeChildren(*setop_node.right, expr_callback, ref_callback);
		break;
	}
	default:
		throw NotImplementedException("QueryNode type not implemented for traversal");
	}

	if (!node.modifiers.empty()) {
		EnumerateQueryNodeModifiers(node, expr_callback);
	}

	for (auto &kv : node.cte_map.map) {
		EnumerateQueryNodeChildren(*kv.second->query->node, expr_callback, ref_callback);
	}
}

} // namespace duckdb
