//===----------------------------------------------------------------------===//
//                         DuckDB
//
// statement_simplifier.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"

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
	void SimplifyReplace(T &element, T &other);

	template <class T>
	void SimplifyListReplace(T &element, vector<T> &list);

	template <class T>
	void SimplifyListReplaceNull(vector<T> &list);

	template <class T>
	void SimplifyOptional(unique_ptr<T> &opt);

	void Simplify(TableRef &ref);

	void Simplify(SelectNode &node);
	void Simplify(SetOperationNode &node);
	void Simplify(QueryNode &node);

	void Simplify(ResultModifier &modifier);
	void Simplify(OrderModifier &modifier);

	void SimplifyExpression(unique_ptr<ParsedExpression> &expr);
	void Simplify(CommonTableExpressionMap &cte_map);
};

} // namespace duckdb
