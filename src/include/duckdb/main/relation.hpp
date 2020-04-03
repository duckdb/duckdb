//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/relation.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/enums/relation_type.hpp"
#include "duckdb/parser/column_definition.hpp"
#include "duckdb/main/query_result.hpp"

#include <memory>

namespace duckdb {
struct BoundStatement;

class ClientContext;
class Binder;
class LogicalOperator;

class Relation : public std::enable_shared_from_this<Relation> {
public:
	Relation(ClientContext &context, RelationType type) : context(context), type(type) {}
	virtual ~Relation(){}

	ClientContext &context;
	RelationType type;
public:
	virtual const vector<ColumnDefinition> &Columns() = 0;
	virtual BoundStatement Bind(Binder &binder) = 0;

	virtual unique_ptr<QueryResult> Execute();
	string ToString();
	virtual string ToString(idx_t depth) = 0;

	void Print();
	void Head(idx_t limit = 10);

	void CreateView(string name);
public:
	// PROJECT
	shared_ptr<Relation> Project(string select_list);
	shared_ptr<Relation> Project(string expression, string alias);
	shared_ptr<Relation> Project(string select_list, vector<string> aliases);
	shared_ptr<Relation> Project(vector<string> expressions, vector<string> aliases);

	// FILTER
	shared_ptr<Relation> Filter(string expression);

	// LIMIT
	shared_ptr<Relation> Limit(int64_t n, int64_t offset = 0);

	// ORDER
	shared_ptr<Relation> Order(string expression);

	// UNION
	shared_ptr<Relation> Union(shared_ptr<Relation> other);

protected:
	string RenderWhitespace(idx_t depth);
};

} // namespace duckdb
