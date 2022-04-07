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
class SelectStatement;
class TableRef;
class SelectNode;
class SetOperationNode;
class QueryNode;
class ParsedExpression;

class StatementSimplifier {
public:
	StatementSimplifier(SelectStatement &statement_p, vector<string> &result_p);

	SelectStatement &statement;
	vector<string> &result;

public:
	void Simplify(SelectStatement &stmt);

private:
	void Simplification();

	template <class T>
	void SimplifyList(vector<T> &list, bool is_optional = true);

	template <class T>
	void SimplifyReplace(T &element, T &other);

	template <class T>
	void SimplifyListReplace(T &element, vector<T> &list);

	template <class T>
	void SimplifyOptional(unique_ptr<T> &opt);

	void Simplify(TableRef &ref);

	void Simplify(SelectNode &node);
	void Simplify(SetOperationNode &node);
	void Simplify(QueryNode &node);

	void SimplifyExpression(unique_ptr<ParsedExpression> &expr);
};

} // namespace duckdb
