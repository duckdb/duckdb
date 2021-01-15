//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/relation.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/enums/join_type.hpp"
#include "duckdb/common/enums/relation_type.hpp"
#include "duckdb/common/winapi.hpp"
#include "duckdb/main/query_result.hpp"
#include "duckdb/parser/column_definition.hpp"

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
	DUCKDB_API Relation(ClientContext &context, RelationType type) : context(context), type(type) {
	}
	DUCKDB_API virtual ~Relation() {
	}

	ClientContext &context;
	RelationType type;

public:
	DUCKDB_API virtual const vector<ColumnDefinition> &Columns() = 0;
	DUCKDB_API virtual unique_ptr<QueryNode> GetQueryNode() = 0;
	DUCKDB_API virtual BoundStatement Bind(Binder &binder);
	DUCKDB_API virtual string GetAlias();

	DUCKDB_API unique_ptr<QueryResult> Execute();
	DUCKDB_API string ToString();
	DUCKDB_API virtual string ToString(idx_t depth) = 0;

	DUCKDB_API void Print();
	DUCKDB_API void Head(idx_t limit = 10);

	DUCKDB_API shared_ptr<Relation> CreateView(string name, bool replace = true);
	DUCKDB_API unique_ptr<QueryResult> Query(string sql);
	DUCKDB_API unique_ptr<QueryResult> Query(string name, string sql);

	//! Explain the query plan of this relation
	DUCKDB_API unique_ptr<QueryResult> Explain();

	DUCKDB_API virtual unique_ptr<TableRef> GetTableRef();
	DUCKDB_API virtual bool IsReadOnly() {
		return true;
	}

public:
	// PROJECT
	DUCKDB_API shared_ptr<Relation> Project(string select_list);
	DUCKDB_API shared_ptr<Relation> Project(string expression, string alias);
	DUCKDB_API shared_ptr<Relation> Project(string select_list, vector<string> aliases);
	DUCKDB_API shared_ptr<Relation> Project(vector<string> expressions);
	DUCKDB_API shared_ptr<Relation> Project(vector<string> expressions, vector<string> aliases);

	// FILTER
	DUCKDB_API shared_ptr<Relation> Filter(string expression);
	DUCKDB_API shared_ptr<Relation> Filter(vector<string> expressions);

	// LIMIT
	DUCKDB_API shared_ptr<Relation> Limit(int64_t n, int64_t offset = 0);

	// ORDER
	DUCKDB_API shared_ptr<Relation> Order(string expression);
	DUCKDB_API shared_ptr<Relation> Order(vector<string> expressions);

	// JOIN operation
	DUCKDB_API shared_ptr<Relation> Join(shared_ptr<Relation> other, string condition, JoinType type = JoinType::INNER);

	// SET operations
	DUCKDB_API shared_ptr<Relation> Union(shared_ptr<Relation> other);
	DUCKDB_API shared_ptr<Relation> Except(shared_ptr<Relation> other);
	DUCKDB_API shared_ptr<Relation> Intersect(shared_ptr<Relation> other);

	// DISTINCT operation
	DUCKDB_API shared_ptr<Relation> Distinct();

	// AGGREGATES
	DUCKDB_API shared_ptr<Relation> Aggregate(string aggregate_list);
	DUCKDB_API shared_ptr<Relation> Aggregate(vector<string> aggregates);
	DUCKDB_API shared_ptr<Relation> Aggregate(string aggregate_list, string group_list);
	DUCKDB_API shared_ptr<Relation> Aggregate(vector<string> aggregates, vector<string> groups);

	// ALIAS
	DUCKDB_API shared_ptr<Relation> Alias(string alias);

	//! Insert the data from this relation into a table
	DUCKDB_API void Insert(string table_name);
	DUCKDB_API void Insert(string schema_name, string table_name);
	//! Insert a row (i.e.,list of values) into a table
	DUCKDB_API void Insert(vector<vector<Value>> values);
	//! Create a table and insert the data from this relation into that table
	DUCKDB_API void Create(string table_name);
	DUCKDB_API void Create(string schema_name, string table_name);

	//! Write a relation to a CSV file
	DUCKDB_API void WriteCSV(string csv_file);

	//! Update a table, can only be used on a TableRelation
	DUCKDB_API virtual void Update(string update, string condition = string());
	//! Delete from a table, can only be used on a TableRelation
	DUCKDB_API virtual void Delete(string condition = string());

public:
	//! Whether or not the relation inherits column bindings from its child or not, only relevant for binding
	DUCKDB_API virtual bool InheritsColumnBindings() {
		return false;
	}
	DUCKDB_API virtual Relation *ChildRelation() {
		return nullptr;
	}

protected:
	DUCKDB_API string RenderWhitespace(idx_t depth);
};

} // namespace duckdb