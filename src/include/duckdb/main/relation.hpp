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
#include "duckdb/common/enums/join_type.hpp"

#include <memory>

namespace duckdb {
struct BoundStatement;

class ClientContext;
class Binder;
class LogicalOperator;
class QueryNode;
class TableRef;

class Relation : public std::enable_shared_from_this<Relation> {
public:
	Relation(ClientContext &context, RelationType type) : context(context), type(type) {
	}
	virtual ~Relation() {
	}

	ClientContext &context;
	RelationType type;

public:
	virtual const vector<ColumnDefinition> &Columns() = 0;
	virtual unique_ptr<QueryNode> GetQueryNode() = 0;
	virtual BoundStatement Bind(Binder &binder);
	virtual string GetAlias();

	unique_ptr<QueryResult> Execute();
	string ToString();
	virtual string ToString(idx_t depth) = 0;

	void Print();
	void Head(idx_t limit = 10);

	shared_ptr<Relation> CreateView(string name, bool replace = true);
	unique_ptr<QueryResult> Query(string sql);
	unique_ptr<QueryResult> Query(string name, string sql);

	//! Explain the query plan of this relation
	unique_ptr<QueryResult> Explain();

	virtual unique_ptr<TableRef> GetTableRef();
	virtual bool IsReadOnly() {
		return true;
	}

public:
	// PROJECT
	shared_ptr<Relation> Project(string select_list);
	shared_ptr<Relation> Project(string expression, string alias);
	shared_ptr<Relation> Project(string select_list, vector<string> aliases);
	shared_ptr<Relation> Project(vector<string> expressions);
	shared_ptr<Relation> Project(vector<string> expressions, vector<string> aliases);

	// FILTER
	shared_ptr<Relation> Filter(string expression);
	shared_ptr<Relation> Filter(vector<string> expressions);

	// LIMIT
	shared_ptr<Relation> Limit(int64_t n, int64_t offset = 0);

	// ORDER
	shared_ptr<Relation> Order(string expression);
	shared_ptr<Relation> Order(vector<string> expressions);

	// JOIN operation
	shared_ptr<Relation> Join(shared_ptr<Relation> other, string condition, JoinType type = JoinType::INNER);

	// SET operations
	shared_ptr<Relation> Union(shared_ptr<Relation> other);
	shared_ptr<Relation> Except(shared_ptr<Relation> other);
	shared_ptr<Relation> Intersect(shared_ptr<Relation> other);

	// DISTINCT operation
	shared_ptr<Relation> Distinct();

	// AGGREGATES
	shared_ptr<Relation> Aggregate(string aggregate_list);
	shared_ptr<Relation> Aggregate(vector<string> aggregates);
	shared_ptr<Relation> Aggregate(string aggregate_list, string group_list);
	shared_ptr<Relation> Aggregate(vector<string> aggregates, vector<string> groups);

	// ALIAS
	shared_ptr<Relation> Alias(string alias);

	//! Insert the data from this relation into a table
	void Insert(string table_name);
	void Insert(string schema_name, string table_name);
	//! Insert a row (i.e.,list of values) into a table
    void Insert(vector<vector<Value>> values);
	//! Create a table and insert the data from this relation into that table
	void Create(string table_name);
	void Create(string schema_name, string table_name);

	//! Write a relation to a CSV file
	void WriteCSV(string csv_file);

	//! Update a table, can only be used on a TableRelation
	virtual void Update(string update, string condition = string());
	//! Delete from a table, can only be used on a TableRelation
	virtual void Delete(string condition = string());

public:
	//! Whether or not the relation inherits column bindings from its child or not, only relevant for binding
	virtual bool InheritsColumnBindings() {
		return false;
	}
	virtual Relation *ChildRelation() {
		return nullptr;
	}

protected:
	string RenderWhitespace(idx_t depth);
};

} // namespace duckdb
