//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/query_node/insert_query_node.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <stdint.h>
#include <string>

#include "duckdb/parser/query_node.hpp"
#include "duckdb/parser/tableref.hpp"
#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/parser/statement/select_statement.hpp"

namespace duckdb {
class Serializer;
class Deserializer;
class OnConflictInfo;
class ExpressionListRef;
class SelectStatement;
enum class InsertColumnOrder : uint8_t;

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

	//! Table name to insert to
	string table;
	//! Schema name to insert to
	string schema;
	//! The catalog name to insert to
	string catalog;

	//! keep track of optional returningList if statement contains a RETURNING keyword
	vector<unique_ptr<ParsedExpression>> returning_list;

	unique_ptr<OnConflictInfo> on_conflict_info;
	unique_ptr<TableRef> table_ref;

	//! Whether or not this a DEFAULT VALUES
	bool default_values = false;

	//! INSERT BY POSITION or INSERT BY NAME
	InsertColumnOrder column_order;

public:
	string ToString() const override;
	bool Equals(const QueryNode *other) const override;
	unique_ptr<QueryNode> Copy() const override;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<QueryNode> Deserialize(Deserializer &deserializer);

	//! If the INSERT statement is inserted DIRECTLY from a values list (i.e. INSERT INTO tbl VALUES (...)) this returns
	//! the expression list Otherwise, this returns NULL
	optional_ptr<ExpressionListRef> GetValuesList() const;
};

} // namespace duckdb
