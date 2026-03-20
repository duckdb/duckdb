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
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/statement/update_statement.hpp"

namespace duckdb {
class Serializer;
class Deserializer;
class ExpressionListRef;

enum class OnConflictAction : uint8_t {
	THROW,
	NOTHING,
	UPDATE,
	REPLACE // Only used in transform/bind step, changed to UPDATE later
};

enum class InsertColumnOrder : uint8_t { INSERT_BY_POSITION = 0, INSERT_BY_NAME = 1 };

class OnConflictInfo {
public:
	OnConflictInfo();

public:
	unique_ptr<OnConflictInfo> Copy() const;

	void Serialize(Serializer &serializer) const;
	static unique_ptr<OnConflictInfo> Deserialize(Deserializer &deserializer);

public:
	OnConflictAction action_type;

	vector<string> indexed_columns;
	//! The SET information (if action_type == UPDATE)
	unique_ptr<UpdateSetInfo> set_info;
	//! The condition determining whether we apply the DO .. for conflicts that arise
	unique_ptr<ParsedExpression> condition;

protected:
	OnConflictInfo(const OnConflictInfo &other);
};

//! InsertQueryNode represents an INSERT DML statement as a QueryNode,
//! enabling serialization and use as a CTE body.
class InsertQueryNode : public QueryNode {
public:
	static constexpr const QueryNodeType TYPE = QueryNodeType::INSERT_QUERY_NODE;

public:
	InsertQueryNode();

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
	//! Secondary table ref kept for ON CONFLICT alias resolution (only set when an ON CONFLICT clause is present)
	unique_ptr<TableRef> table_ref;
	//! Whether or not this is DEFAULT VALUES
	bool default_values = false;
	//! INSERT BY POSITION or INSERT BY NAME
	InsertColumnOrder column_order = InsertColumnOrder::INSERT_BY_POSITION;

public:
	static string OnConflictActionToString(OnConflictAction action);

	//! If the INSERT is directly from a values list (INSERT INTO tbl VALUES (...)) returns the ExpressionListRef.
	//! Otherwise returns nullptr.
	optional_ptr<ExpressionListRef> GetValuesList() const;

	string ToString() const override;
	bool Equals(const QueryNode *other) const override;
	unique_ptr<QueryNode> Copy() const override;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<QueryNode> Deserialize(Deserializer &deserializer);
};

} // namespace duckdb
