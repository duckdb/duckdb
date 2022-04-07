#include "statement_simplifier.hpp"

#ifndef DUCKDB_AMALGAMATION
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/query_node/set_operation_node.hpp"
#include "duckdb/parser/tableref/list.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/parser/expression/list.hpp"
#endif

namespace duckdb {

StatementSimplifier::StatementSimplifier(SelectStatement &statement_p, vector<string> &result_p) :
	statement(statement_p), result(result_p) {}


void StatementSimplifier::Simplification() {
	result.push_back(statement.ToString());
}

template<class T>
void StatementSimplifier::SimplifyList(vector<T> &list, bool is_optional) {
	if (list.size() <= (is_optional ? 0 : 1)) {
		return;
	}
	for(idx_t i = 0; i < list.size(); i++) {
		auto n = move(list[i]);
		list.erase(list.begin() + i);
		Simplification();
		list.insert(list.begin() + i, move(n));
	}
}

template<class T>
void StatementSimplifier::SimplifyReplace(T &element, T &other) {
	auto n = move(element);
	element = move(other);
	Simplification();
	other = move(element);
	element = move(n);
}

template<class T>
void StatementSimplifier::SimplifyListReplace(T &element, vector<T> &list) {
	for(idx_t i = 0; i < list.size(); i++) {
		SimplifyReplace(element, list[i]);
	}
}

template<class T>
void StatementSimplifier::SimplifyOptional(unique_ptr<T> &opt) {
	if (!opt) {
		return;
	}
	auto n = move(opt);
	Simplification();
	opt = move(n);
}

void StatementSimplifier::Simplify(TableRef &ref) {
	switch(ref.type) {
	case TableReferenceType::SUBQUERY: {
		auto &subquery = (SubqueryRef &) ref;
		Simplify(*subquery.subquery->node);
		break;
	}
	case TableReferenceType::CROSS_PRODUCT: {
		auto &cp = (CrossProductRef &) ref;
		Simplify(*cp.left);
		Simplify(*cp.right);
		break;
	}
	case TableReferenceType::JOIN: {
		auto &cp = (JoinRef &) ref;
		Simplify(*cp.left);
		Simplify(*cp.right);
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
	Simplify(*node.from_table);
	// simplify groups
	SimplifyList(node.groups.grouping_sets);
	// simplify filters
	SimplifyOptional(node.where_clause);
	SimplifyOptional(node.having);
	SimplifyOptional(node.qualify);
	SimplifyOptional(node.sample);
}

void StatementSimplifier::Simplify(SetOperationNode &node) {
	Simplify(*node.left);
	Simplify(*node.right);
}

void StatementSimplifier::Simplify(QueryNode &node) {
	// remove individual CTEs
	vector<string> cte_keys;
	for(auto &kv : node.cte_map) {
		cte_keys.push_back(kv.first);
	}
	for(idx_t i = 0; i < cte_keys.size(); i++) {
		auto n = move(node.cte_map[cte_keys[i]]);
		node.cte_map.erase(cte_keys[i]);
		Simplification();
		node.cte_map[cte_keys[i]] = move(n);

		// simplify individual ctes
		Simplify(*node.cte_map[cte_keys[i]]->query->node);
	}
	switch(node.type) {
	case QueryNodeType::SELECT_NODE:
		Simplify((SelectNode &) node);
		break;
	case QueryNodeType::SET_OPERATION_NODE:
		Simplify((SetOperationNode &) node);
		break;
	case QueryNodeType::RECURSIVE_CTE_NODE:
	default:
		break;
	}
	SimplifyList(node.modifiers);
}

void StatementSimplifier::SimplifyExpression(unique_ptr<ParsedExpression> &expr) {
	switch(expr->GetExpressionClass()) {
	case ExpressionClass::CONJUNCTION: {
		auto &conj = (ConjunctionExpression &) *expr;
		SimplifyListReplace(expr, conj.children);
		break;
	}
	case ExpressionClass::FUNCTION: {
		auto &func = (FunctionExpression &) *expr;
		SimplifyListReplace(expr, func.children);
		break;
	}
	case ExpressionClass::OPERATOR: {
		auto &op = (OperatorExpression &) *expr;
		SimplifyListReplace(expr, op.children);
		break;
	}
	case ExpressionClass::CASE: {
		auto &op = (CaseExpression &) *expr;
		SimplifyReplace(expr, op.else_expr);
		for(auto &case_check : op.case_checks) {
			SimplifyReplace(expr, case_check.then_expr);
			SimplifyReplace(expr, case_check.when_expr);
		}
		break;
	}
	case ExpressionClass::CAST: {
		auto &cast = (CastExpression &) *expr;
		SimplifyReplace(expr, cast.child);
		break;
	}
	case ExpressionClass::COLLATE: {
		auto &collate = (CollateExpression &) *expr;
		SimplifyReplace(expr, collate.child);
		break;
	}
	default:
		break;
	}
}

void StatementSimplifier::Simplify(SelectStatement &stmt) {
	Simplify(*stmt.node);
	ParsedExpressionIterator::EnumerateQueryNodeChildren(*stmt.node, [&](unique_ptr<ParsedExpression> &child) {
		SimplifyExpression(child);
	});
}

}
