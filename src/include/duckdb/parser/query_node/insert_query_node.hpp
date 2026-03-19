//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/query_node/insert_query_node.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/query_node.hpp"
#include "duckdb/parser/tableref.hpp"
#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/parser/statement/insert_statement.hpp"

namespace duckdb {

//! InsertQueryNode represents an INSERT DML statement as a QueryNode,
//! enabling serialization and use as a CTE body.
class InsertQueryNode : public QueryNode {
public:
	static constexpr const QueryNodeType TYPE = QueryNodeType::INSERT_QUERY_NODE;

public:
	InsertQueryNode();
	explicit InsertQueryNode(InsertStatement &stmt);

	//! The select statement to insert from
	unique_ptr<SelectStatement> select_statement;
	//! Column names to insert into
	vector<string> columns;
	//! The target table reference (carries catalog, schema, table name)
	unique_ptr<TableRef> table;
	//! keep track of optional returningList if statement contains a RETURNING keyword
	vector<unique_ptr<ParsedExpression>> returning_list;
	//! ON CONFLICT information
	unique_ptr<OnConflictInfo> on_conflict_info;
	//! Direct table ref (used when table is resolved via table_ref clause)
	unique_ptr<TableRef> table_ref;
	//! Whether or not this is DEFAULT VALUES
	bool default_values = false;
	//! INSERT BY POSITION or INSERT BY NAME
	InsertColumnOrder column_order = InsertColumnOrder::INSERT_BY_POSITION;

public:
	string ToString() const override;
	bool Equals(const QueryNode *other) const override;
	unique_ptr<QueryNode> Copy() const override;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<QueryNode> Deserialize(Deserializer &deserializer);
};

} // namespace duckdb
