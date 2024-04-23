//===----------------------------------------------------------------------===//
//                         DuckDB
//
// statement_simplifier.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include "duckdb/parser/query_node.hpp"

namespace duckdb {
class SQLStatement;
class SelectStatement;
class InsertStatement;
class UpdateStatement;
class DeleteStatement;
class TableRef;
class SelectNode;
class SetOperationNode;
class QueryNode;
class ParsedExpression;
class ResultModifier;
class OrderModifier;
class UpdateSetInfo;
class GroupByNode;

class StatementSimplifier {
public:
	StatementSimplifier(SQLStatement &statement_p, vector<string> &result_p);

	SQLStatement &statement;
	vector<string> &result;

public:
	void Simplify(SQLStatement &stmt);

private:
	void Simplify(SelectStatement &stmt);
	void Simplify(InsertStatement &stmt);
	void Simplify(UpdateStatement &stmt);
	void Simplify(DeleteStatement &stmt);

	void Simplification();

	template <class T>
	void SimplifyList(vector<T> &list, bool is_optional = true);
	template <class T>
	void SimplifyMap(T &map);
	template <class T>
	void SimplifySet(T &set);

	template <class T>
	void SimplifyReplace(T &element, T &other);

	template <class T>
	void SimplifyOptional(duckdb::unique_ptr<T> &opt);
	template <class T>
	void SimplifyAlias(T &input);
	template <class T>
	void SimplifyEnum(T &enum_ref, T default_value);

	void Simplify(unique_ptr<TableRef> &ref);

	void Simplify(SelectNode &node);
	void Simplify(SetOperationNode &node);
	void Simplify(unique_ptr<QueryNode> &node);

	void Simplify(ResultModifier &modifier);
	void Simplify(OrderModifier &modifier);

	void SimplifyExpression(duckdb::unique_ptr<ParsedExpression> &expr);
	void SimplifyOptionalExpression(duckdb::unique_ptr<ParsedExpression> &expr);
	void SimplifyChildExpression(duckdb::unique_ptr<ParsedExpression> &expr, unique_ptr<ParsedExpression> &child);
	void SimplifyExpressionList(duckdb::unique_ptr<ParsedExpression> &expr,
	                            vector<unique_ptr<ParsedExpression>> &expression_list);
	void SimplifyExpressionList(vector<unique_ptr<ParsedExpression>> &expression_list, bool is_optional = true);
	void Simplify(CommonTableExpressionMap &cte_map);
	void Simplify(GroupByNode &groups);

	void Simplify(UpdateSetInfo &info);

private:
	vector<reference<unique_ptr<QueryNode>>> query_nodes;
};

} // namespace duckdb
