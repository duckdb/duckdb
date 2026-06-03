//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/statement/connect_execute_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/sql_statement.hpp"

namespace duckdb {

//! `CONNECT <name> EXECUTE` — the marker form. Carries only the target catalog name; the
//! *following* peeled statement is the verbatim payload that the iterator/loop ships to
//! <target>. This statement itself has no terminator in the grammar; the next peel runs as
//! a `PASSTHROUGH_STATEMENT` (or whatever the iterator scans out before the next `;`).
class ConnectExecuteStatement : public SQLStatement {
public:
	static constexpr const StatementType TYPE = StatementType::CONNECT_EXECUTE_STATEMENT;

public:
	ConnectExecuteStatement();
	explicit ConnectExecuteStatement(string target);

	//! Catalog name to dispatch the next peel against. Empty when `target_is_local` is true.
	string target;
	//! True if the user typed the `LOCAL` keyword (run the next peel locally). When true,
	//! `target` is empty; otherwise it carries the catalog identifier.
	bool target_is_local = false;

protected:
	ConnectExecuteStatement(const ConnectExecuteStatement &other);

public:
	unique_ptr<SQLStatement> Copy() const override;
	string ToString() const override;
};

} // namespace duckdb
