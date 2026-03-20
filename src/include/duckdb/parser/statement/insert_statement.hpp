//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/statement/insert_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/query_node/insert_query_node.hpp"

namespace duckdb {
class ExpressionListRef;

class InsertStatement : public SQLStatement {
public:
	static constexpr const StatementType TYPE = StatementType::INSERT_STATEMENT;

public:
	InsertStatement();

	//! The INSERT query node holding all statement data
	unique_ptr<InsertQueryNode> node;

protected:
	InsertStatement(const InsertStatement &other);

public:
	// Delegate to InsertQueryNode for backward compatibility
	static string OnConflictActionToString(OnConflictAction action) {
		return InsertQueryNode::OnConflictActionToString(action);
	}

	string ToString() const override;
	unique_ptr<SQLStatement> Copy() const override;

	//! If the INSERT statement is inserted DIRECTLY from a values list (i.e. INSERT INTO tbl VALUES (...)) this returns
	//! the expression list. Otherwise, this returns NULL.
	optional_ptr<ExpressionListRef> GetValuesList() const;
};

} // namespace duckdb
