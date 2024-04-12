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
void StatementSimplifier::SimplifyMap(T &map) {
	if (map.empty()) {
		return;
	}
	// copy the keys
	vector<typename T::key_type> keys;
	for (auto &entry : map) {
		keys.push_back(entry.first);
	}
	// try to remove all of the keys
	for (idx_t i = 0; i < keys.size(); i++) {
		auto entry = map.find(keys[i]);
		auto n = std::move(entry->second);
		map.erase(entry);
		Simplification();
		map.insert(make_pair(std::move(keys[i]), std::move(n)));
	}
}

template <class T>
void StatementSimplifier::SimplifySet(T &set) {
	if (set.empty()) {
		return;
	}
	// copy the keys
	vector<typename T::key_type> keys;
	for (auto &entry : set) {
		keys.push_back(entry);
	}
	// try to remove all of the keys
	for (idx_t i = 0; i < keys.size(); i++) {
		auto entry = set.find(keys[i]);
		set.erase(entry);
		Simplification();
		set.insert(std::move(keys[i]));
	}
}

template <class T>
void StatementSimplifier::SimplifyOptional(duckdb::unique_ptr<T> &opt) {
	if (!opt) {
		return;
	}
	auto n = std::move(opt);
	Simplification();
	opt = std::move(n);
}

template <class T>
void StatementSimplifier::SimplifyEnum(T &enum_ref, T default_value) {
	if (enum_ref == default_value) {
		return;
	}
	auto current = enum_ref;
	enum_ref = default_value;
	Simplification();
	enum_ref = current;
}

template <class T>
void StatementSimplifier::SimplifyAlias(T &input) {
	auto alias = std::move(input.alias);
	auto column_name_alias = std::move(input.column_name_alias);
	Simplification();
	input.alias = std::move(alias);
	input.column_name_alias = std::move(column_name_alias);
}

void StatementSimplifier::Simplify(unique_ptr<TableRef> &ref) {
	switch (ref->type) {
	case TableReferenceType::SUBQUERY: {
		auto &subquery = ref->Cast<SubqueryRef>();
		SimplifyAlias(subquery);
		if (subquery.subquery->node->type == QueryNodeType::SELECT_NODE) {
			auto &select_node = subquery.subquery->node->Cast<SelectNode>();
			SimplifyReplace(ref, select_node.from_table);
		}
		Simplify(subquery.subquery->node);
		break;
	}
	case TableReferenceType::JOIN: {
		auto &cp = ref->Cast<JoinRef>();
		Simplify(cp.left);
		Simplify(cp.right);
		SimplifyOptionalExpression(cp.condition);
		SimplifyReplace(ref, cp.left);
		SimplifyReplace(ref, cp.right);
		break;
	}
	case TableReferenceType::EXPRESSION_LIST: {
		auto &expr_list = ref->Cast<ExpressionListRef>();
		if (expr_list.values.size() == 1) {
			SimplifyList(expr_list.values[0]);
		} else if (expr_list.values.size() > 1) {
			SimplifyList(expr_list.values, false);
		}
		break;
	}
	case TableReferenceType::TABLE_FUNCTION: {
		auto &table_function = ref->Cast<TableFunctionRef>();
		// try to remove aliases
		SimplifyAlias(table_function);
		break;
	}
	case TableReferenceType::BASE_TABLE: {
		auto &table_ref = ref->Cast<BaseTableRef>();
		SimplifyAlias(table_ref);
		break;
	}
	default:
		break;
	}
}

void StatementSimplifier::Simplify(GroupByNode &groups) {
	// try to remove all groups
	auto group_expr = std::move(groups.group_expressions);
	auto group_sets = std::move(groups.grouping_sets);
	Simplification();
	groups.group_expressions = std::move(group_expr);
	groups.grouping_sets = std::move(group_sets);

	// try to remove grouping sets
	SimplifyList(groups.grouping_sets, false);
	// simplify expressions
	for (auto &group : groups.group_expressions) {
		SimplifyExpression(group);
	}
}

void StatementSimplifier::Simplify(SelectNode &node) {
	// simplify projection list
	SimplifyExpressionList(node.select_list, false);
	// from clause
	SimplifyOptional(node.from_table);
	// simplify groups
	Simplify(node.groups);
	// simplify filters
	SimplifyOptionalExpression(node.where_clause);
	SimplifyOptionalExpression(node.having);
	SimplifyOptionalExpression(node.qualify);
	SimplifyOptional(node.sample);
	SimplifyEnum(node.aggregate_handling, AggregateHandling::STANDARD_HANDLING);

	Simplify(node.from_table);
}

void StatementSimplifier::Simplify(SetOperationNode &node) {
	Simplify(node.left);
	Simplify(node.right);
}

void StatementSimplifier::Simplify(CommonTableExpressionMap &cte) {
	// remove individual CTEs
	SimplifyMap(cte.map);
	for (auto &cte_child : cte.map) {
		// simplify individual ctes
		Simplify(cte_child.second->query->node);
	}
}

void StatementSimplifier::Simplify(unique_ptr<QueryNode> &node) {
	query_nodes.push_back(node);
	Simplify(node->cte_map);
	switch (node->type) {
	case QueryNodeType::SELECT_NODE:
		Simplify(node->Cast<SelectNode>());
		break;
	case QueryNodeType::SET_OPERATION_NODE: {
		auto &setop = node->Cast<SetOperationNode>();
		SimplifyReplace(node, setop.left);
		SimplifyReplace(node, setop.right);
		Simplify(setop);
		break;
	}
	case QueryNodeType::RECURSIVE_CTE_NODE:
	case QueryNodeType::CTE_NODE:
	default:
		break;
	}
	for (auto &modifier : node->modifiers) {
		Simplify(*modifier);
	}
	SimplifyList(node->modifiers);
	query_nodes.pop_back();
}

void StatementSimplifier::SimplifyExpressionList(duckdb::unique_ptr<ParsedExpression> &expr,
                                                 vector<unique_ptr<ParsedExpression>> &expression_list) {
	for (auto &child : expression_list) {
		SimplifyChildExpression(expr, child);
	}
}

void StatementSimplifier::SimplifyExpressionList(vector<unique_ptr<ParsedExpression>> &expression_list,
                                                 bool is_optional) {
	SimplifyList(expression_list, is_optional);
	for (auto &child : expression_list) {
		SimplifyExpression(child);
	}
}

void StatementSimplifier::SimplifyChildExpression(duckdb::unique_ptr<ParsedExpression> &expr,
                                                  unique_ptr<ParsedExpression> &child) {
	if (!child) {
		return;
	}
	SimplifyReplace(expr, child);
	SimplifyExpression(child);
}

void StatementSimplifier::SimplifyOptionalExpression(duckdb::unique_ptr<ParsedExpression> &expr) {
	if (!expr) {
		return;
	}
	SimplifyOptional(expr);
	SimplifyExpression(expr);
}

void StatementSimplifier::SimplifyExpression(duckdb::unique_ptr<ParsedExpression> &expr) {
	if (!expr) {
		return;
	}
	auto expr_class = expr->GetExpressionClass();
	switch (expr_class) {
	case ExpressionClass::CONSTANT:
		return;
	default:
		break;
	}
	duckdb::unique_ptr<ParsedExpression> constant = make_uniq<ConstantExpression>(Value());
	SimplifyReplace(expr, constant);
	switch (expr_class) {
	case ExpressionClass::CONJUNCTION: {
		auto &conj = expr->Cast<ConjunctionExpression>();
		SimplifyExpressionList(expr, conj.children);
		break;
	}
	case ExpressionClass::FUNCTION: {
		auto &func = expr->Cast<FunctionExpression>();
		SimplifyExpressionList(expr, func.children);
		break;
	}
	case ExpressionClass::OPERATOR: {
		auto &op = expr->Cast<OperatorExpression>();
		SimplifyExpressionList(expr, op.children);
		break;
	}
	case ExpressionClass::CASE: {
		auto &op = expr->Cast<CaseExpression>();
		SimplifyChildExpression(expr, op.else_expr);
		for (auto &case_check : op.case_checks) {
			SimplifyChildExpression(expr, case_check.then_expr);
			SimplifyChildExpression(expr, case_check.when_expr);
		}
		break;
	}
	case ExpressionClass::CAST: {
		auto &cast = expr->Cast<CastExpression>();
		SimplifyChildExpression(expr, cast.child);
		break;
	}
	case ExpressionClass::COLLATE: {
		auto &collate = expr->Cast<CollateExpression>();
		SimplifyChildExpression(expr, collate.child);
		break;
	}
	case ExpressionClass::SUBQUERY: {
		auto &subq = expr->Cast<SubqueryExpression>();
		// try to move this subquery fully into the outer query
		if (!query_nodes.empty()) {
			SimplifyReplace(query_nodes.back().get(), subq.subquery->node);
		}
		SimplifyChildExpression(expr, subq.child);
		Simplify(subq.subquery->node);
		break;
	}
	case ExpressionClass::COMPARISON: {
		auto &comp = expr->Cast<ComparisonExpression>();
		SimplifyChildExpression(expr, comp.left);
		SimplifyChildExpression(expr, comp.right);
		break;
	}
	case ExpressionClass::STAR: {
		auto &star = expr->Cast<StarExpression>();
		SimplifyMap(star.replace_list);
		SimplifySet(star.exclude_list);
		for (auto &entry : star.replace_list) {
			SimplifyChildExpression(expr, entry.second);
		}
		break;
	}
	case ExpressionClass::WINDOW: {
		auto &window = expr->Cast<WindowExpression>();
		SimplifyExpressionList(expr, window.children);
		SimplifyExpressionList(expr, window.partitions);
		SimplifyList(window.orders);
		SimplifyChildExpression(expr, window.filter_expr);
		SimplifyChildExpression(expr, window.start_expr);
		SimplifyChildExpression(expr, window.end_expr);
		SimplifyChildExpression(expr, window.offset_expr);
		SimplifyChildExpression(expr, window.default_expr);
		SimplifyEnum(window.ignore_nulls, false);
		SimplifyEnum(window.distinct, false);
		SimplifyEnum(window.start, WindowBoundary::INVALID);
		SimplifyEnum(window.end, WindowBoundary::INVALID);
		SimplifyEnum(window.exclude_clause, WindowExcludeMode::NO_OTHER);
		break;
	}
	default:
		break;
	}
}

void StatementSimplifier::Simplify(ResultModifier &modifier) {
	switch (modifier.type) {
	case ResultModifierType::ORDER_MODIFIER:
		Simplify(modifier.Cast<OrderModifier>());
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
	Simplify(stmt.node);
	ParsedExpressionIterator::EnumerateQueryNodeChildren(
	    *stmt.node, [&](duckdb::unique_ptr<ParsedExpression> &child) { SimplifyExpression(child); });
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
	SimplifyOptional(stmt.from_table);
	D_ASSERT(stmt.set_info);
	Simplify(*stmt.set_info);
	SimplifyList(stmt.returning_list);
}

void StatementSimplifier::Simplify(SQLStatement &stmt) {
	switch (stmt.type) {
	case StatementType::SELECT_STATEMENT:
		Simplify(stmt.Cast<SelectStatement>());
		break;
	case StatementType::INSERT_STATEMENT:
		Simplify(stmt.Cast<InsertStatement>());
		break;
	case StatementType::UPDATE_STATEMENT:
		Simplify(stmt.Cast<UpdateStatement>());
		break;
	case StatementType::DELETE_STATEMENT:
		Simplify(stmt.Cast<DeleteStatement>());
		break;
	default:
		throw InvalidInputException("Expected a single SELECT, INSERT or UPDATE statement");
	}
}

} // namespace duckdb
