#include "statement_simplifier.hpp"

#ifndef DUCKDB_AMALGAMATION
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/query_node/set_operation_node.hpp"
#include "duckdb/parser/tableref/list.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/parser/expression/list.hpp"
#include "duckdb/parser/statement/delete_statement.hpp"
#include "duckdb/parser/statement/insert_statement.hpp"
#include "duckdb/parser/statement/update_statement.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#endif

namespace duckdb {

StatementSimplifier::StatementSimplifier(SQLStatement &statement_p, vector<string> &result_p)
    : statement(statement_p), result(result_p) {
}

void StatementSimplifier::Simplification() {
	result.push_back(statement.ToString());
}

template <class T>
void StatementSimplifier::SimplifyReplace(T &element, T &other) {
	auto n = std::move(element);
	element = std::move(other);
	Simplification();
	other = std::move(element);
	element = std::move(n);
}

template <class T>
void StatementSimplifier::SimplifyList(vector<T> &list, bool is_optional) {
	if (list.size() <= (is_optional ? 0 : 1)) {
		return;
	}
	for (idx_t i = 0; i < list.size(); i++) {
		auto n = std::move(list[i]);
		list.erase(list.begin() + i);
		Simplification();
		list.insert(list.begin() + i, std::move(n));
	}
}

template <class T>
void StatementSimplifier::SimplifyListReplaceNull(vector<T> &list) {
	for (idx_t i = 0; i < list.size(); i++) {
		unique_ptr<ParsedExpression> constant = make_unique<ConstantExpression>(Value());
		SimplifyReplace(list[i], constant);
	}
}

template <class T>
void StatementSimplifier::SimplifyListReplace(T &element, vector<T> &list) {
	for (idx_t i = 0; i < list.size(); i++) {
		SimplifyReplace(element, list[i]);
	}
}

template <class T>
void StatementSimplifier::SimplifyOptional(unique_ptr<T> &opt) {
	if (!opt) {
		return;
	}
	auto n = std::move(opt);
	Simplification();
	opt = std::move(n);
}

void StatementSimplifier::Simplify(TableRef &ref) {
	switch (ref.type) {
	case TableReferenceType::SUBQUERY: {
		auto &subquery = (SubqueryRef &)ref;
		Simplify(*subquery.subquery->node);
		break;
	}
	case TableReferenceType::CROSS_PRODUCT: {
		auto &cp = (CrossProductRef &)ref;
		Simplify(*cp.left);
		Simplify(*cp.right);
		break;
	}
	case TableReferenceType::JOIN: {
		auto &cp = (JoinRef &)ref;
		Simplify(*cp.left);
		Simplify(*cp.right);
		break;
	}
	case TableReferenceType::EXPRESSION_LIST: {
		auto &expr_list = (ExpressionListRef &)ref;
		if (expr_list.values.size() == 1) {
			SimplifyList(expr_list.values[0]);
		} else if (expr_list.values.size() > 1) {
			SimplifyList(expr_list.values, false);
		}
		break;
	}
	default:
		break;
	}
}

void StatementSimplifier::Simplify(SelectNode &node) {
	// simplify projection list
	SimplifyList(node.select_list, false);
	// from clause
	SimplifyOptional(node.from_table);
	// simplify groups
	SimplifyList(node.groups.grouping_sets);
	// simplify filters
	SimplifyOptional(node.where_clause);
	SimplifyOptional(node.having);
	SimplifyOptional(node.qualify);
	SimplifyOptional(node.sample);

	Simplify(*node.from_table);
}

void StatementSimplifier::Simplify(SetOperationNode &node) {
	Simplify(*node.left);
	Simplify(*node.right);
}

void StatementSimplifier::Simplify(CommonTableExpressionMap &cte) {
	// remove individual CTEs
	vector<string> cte_keys;
	for (auto &kv : cte.map) {
		cte_keys.push_back(kv.first);
	}
	for (idx_t i = 0; i < cte_keys.size(); i++) {
		auto n = std::move(cte.map[cte_keys[i]]);
		cte.map.erase(cte_keys[i]);
		Simplification();
		cte.map[cte_keys[i]] = std::move(n);

		// simplify individual ctes
		Simplify(*cte.map[cte_keys[i]]->query->node);
	}
}

void StatementSimplifier::Simplify(QueryNode &node) {
	Simplify(node.cte_map);
	switch (node.type) {
	case QueryNodeType::SELECT_NODE:
		Simplify((SelectNode &)node);
		break;
	case QueryNodeType::SET_OPERATION_NODE:
		Simplify((SetOperationNode &)node);
		break;
	case QueryNodeType::RECURSIVE_CTE_NODE:
	default:
		break;
	}
	for (auto &modifier : node.modifiers) {
		Simplify(*modifier);
	}
	SimplifyList(node.modifiers);
}

void StatementSimplifier::SimplifyExpression(unique_ptr<ParsedExpression> &expr) {
	if (!expr) {
		return;
	}
	auto expr_class = expr->GetExpressionClass();
	switch (expr_class) {
	case ExpressionClass::COLUMN_REF:
	case ExpressionClass::CONSTANT:
		return;
	default:
		break;
	}
	unique_ptr<ParsedExpression> constant = make_unique<ConstantExpression>(Value());
	SimplifyReplace(expr, constant);
	switch (expr_class) {
	case ExpressionClass::CONJUNCTION: {
		auto &conj = (ConjunctionExpression &)*expr;
		SimplifyListReplace(expr, conj.children);
		break;
	}
	case ExpressionClass::FUNCTION: {
		auto &func = (FunctionExpression &)*expr;
		SimplifyListReplace(expr, func.children);
		SimplifyListReplaceNull(func.children);
		break;
	}
	case ExpressionClass::OPERATOR: {
		auto &op = (OperatorExpression &)*expr;
		SimplifyListReplace(expr, op.children);
		break;
	}
	case ExpressionClass::CASE: {
		auto &op = (CaseExpression &)*expr;
		SimplifyReplace(expr, op.else_expr);
		for (auto &case_check : op.case_checks) {
			SimplifyReplace(expr, case_check.then_expr);
			SimplifyReplace(expr, case_check.when_expr);
		}
		break;
	}
	case ExpressionClass::CAST: {
		auto &cast = (CastExpression &)*expr;
		SimplifyReplace(expr, cast.child);
		break;
	}
	case ExpressionClass::COLLATE: {
		auto &collate = (CollateExpression &)*expr;
		SimplifyReplace(expr, collate.child);
		break;
	}
	default:
		break;
	}
}

void StatementSimplifier::Simplify(ResultModifier &modifier) {
	switch (modifier.type) {
	case ResultModifierType::ORDER_MODIFIER:
		Simplify((OrderModifier &)modifier);
		break;
	default:
		break;
	}
}

void StatementSimplifier::Simplify(OrderModifier &modifier) {
	for (auto &order : modifier.orders) {
		SimplifyExpression(order.expression);
	}
	SimplifyList(modifier.orders);
}

void StatementSimplifier::Simplify(SelectStatement &stmt) {
	Simplify(*stmt.node);
	ParsedExpressionIterator::EnumerateQueryNodeChildren(
	    *stmt.node, [&](unique_ptr<ParsedExpression> &child) { SimplifyExpression(child); });
}

void StatementSimplifier::Simplify(InsertStatement &stmt) {
	Simplify(stmt.cte_map);
	Simplify(*stmt.select_statement);
	SimplifyList(stmt.returning_list);
}

void StatementSimplifier::Simplify(DeleteStatement &stmt) {
	Simplify(stmt.cte_map);
	SimplifyOptional(stmt.condition);
	SimplifyExpression(stmt.condition);
	SimplifyList(stmt.using_clauses);
	SimplifyList(stmt.returning_list);
}

void StatementSimplifier::Simplify(UpdateSetInfo &info) {
	SimplifyOptional(info.condition);
	SimplifyExpression(info.condition);
	if (info.columns.size() > 1) {
		for (idx_t i = 0; i < info.columns.size(); i++) {
			auto col = std::move(info.columns[i]);
			auto expr = std::move(info.expressions[i]);
			info.columns.erase(info.columns.begin() + i);
			info.expressions.erase(info.expressions.begin() + i);
			Simplification();
			info.columns.insert(info.columns.begin() + i, std::move(col));
			info.expressions.insert(info.expressions.begin() + i, std::move(expr));
		}
	}
	for (auto &expr : info.expressions) {
		SimplifyExpression(expr);
	}
}

void StatementSimplifier::Simplify(UpdateStatement &stmt) {
	Simplify(stmt.cte_map);
	if (stmt.from_table) {
		Simplify(*stmt.from_table);
	}
	D_ASSERT(stmt.set_info);
	Simplify(*stmt.set_info);
	SimplifyList(stmt.returning_list);
}

void StatementSimplifier::Simplify(SQLStatement &stmt) {
	switch (stmt.type) {
	case StatementType::SELECT_STATEMENT:
		Simplify((SelectStatement &)stmt);
		break;
	case StatementType::INSERT_STATEMENT:
		Simplify((InsertStatement &)stmt);
		break;
	case StatementType::UPDATE_STATEMENT:
		Simplify((UpdateStatement &)stmt);
		break;
	case StatementType::DELETE_STATEMENT:
		Simplify((DeleteStatement &)stmt);
		break;
	default:
		throw InvalidInputException("Expected a single SELECT, INSERT or UPDATE statement");
	}
}

} // namespace duckdb
