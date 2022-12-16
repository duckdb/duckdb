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

namespace duckdb {
class ExpressionListRef;

// Which action should we perform when a insert conflict occurs
enum class InsertConflictActionType : uint8_t { THROW, NOTHING, UPDATE };

class InsertStatement : public SQLStatement {
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

	//! keep track of optional returningList if statement contains a RETURNING keyword
	vector<unique_ptr<ParsedExpression>> returning_list;

	//! Action to perform if the insert statement fails because of a conflict
	InsertConflictActionType action_type;

	//! CTEs
	CommonTableExpressionMap cte_map;

protected:
	InsertStatement(const InsertStatement &other);

public:
	string ToString() const override;
	unique_ptr<SQLStatement> Copy() const override;

	//! If the INSERT statement is inserted DIRECTLY from a values list (i.e. INSERT INTO tbl VALUES (...)) this returns
	//! the expression list Otherwise, this returns NULL
	ExpressionListRef *GetValuesList() const;
};

} // namespace duckdb
