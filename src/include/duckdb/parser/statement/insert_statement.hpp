//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/statement/insert_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/query_node.hpp"
#include "duckdb/parser/statement/update_statement.hpp"

namespace duckdb {
class ExpressionListRef;
class UpdateSetInfo;

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

class InsertStatement : public SQLStatement {
public:
	static constexpr const StatementType TYPE = StatementType::INSERT_STATEMENT;

public:
	InsertStatement();

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

	//! CTEs
	CommonTableExpressionMap cte_map;

	//! Whether or not this a DEFAULT VALUES
	bool default_values = false;

	//! INSERT BY POSITION or INSERT BY NAME
	InsertColumnOrder column_order = InsertColumnOrder::INSERT_BY_POSITION;

protected:
	InsertStatement(const InsertStatement &other);

public:
	static string OnConflictActionToString(OnConflictAction action);

	string ToString() const override;
	unique_ptr<SQLStatement> Copy() const override;

	//! If the INSERT statement is inserted DIRECTLY from a values list (i.e. INSERT INTO tbl VALUES (...)) this returns
	//! the expression list Otherwise, this returns NULL
	optional_ptr<ExpressionListRef> GetValuesList() const;
};

} // namespace duckdb
