//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/statement/passthrough_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/sql_statement.hpp"

namespace duckdb {

//! Marker statement emitted by Connection::ExtractStatements for Layer 1 EXECUTE chunks
//! (including RAW-while-bound chunks, which the extract layer rewrites as EXECUTE against the
//! current binding). Carries the target catalog name and the verbatim payload SQL. The binder
//! resolves the target via DatabaseManager, calls `Catalog::RemoteExecute(payload)` to get a
//! TableRef, wraps it as a SelectStatement, and binds the wrapped statement. Resolution at bind
//! time mirrors how the binder already handles deferred name lookups (table references), so
//! catalogs attached between Extract and Prepare/Bind are picked up.
class PassthroughStatement : public SQLStatement {
public:
	static constexpr const StatementType TYPE = StatementType::PASSTHROUGH_STATEMENT;

public:
	PassthroughStatement(string target_p, string payload_p);

	//! Catalog name to dispatch against (e.g. "pg", "qk").
	string target;
	//! Verbatim SQL payload — forwarded to the backend as a string. NEVER parsed locally.
	string payload;

protected:
	PassthroughStatement(const PassthroughStatement &other);

public:
	unique_ptr<SQLStatement> Copy() const override;
	string ToString() const override;
};

} // namespace duckdb
