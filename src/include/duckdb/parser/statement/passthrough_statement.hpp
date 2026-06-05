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

//! Emitted by StatementIterator immediately after a CONNECT_EXECUTE marker. Carries one of two
//! payloads depending on what the marker asked for:
//!
//!   - `CONNECT <name> EXECUTE …` → remote dispatch. `target` is the catalog name; `payload` is
//!     the verbatim source slice of the inner peel. Binding resolves the target catalog and
//!     calls `Catalog::RemoteExecute(payload)`. The local SQL parser never interprets the
//!     payload text.
//!
//!   - `CONNECT LOCAL EXECUTE …` → forced-local dispatch. `target_is_local` is true and
//!     `local_statement` carries the already-parsed inner statement. Binding just delegates to
//!     `Bind(*local_statement)`. The wrapper exists so the ClientContext::Query() chokepoint
//!     lets the statement past its "if connected, re-route everything to the sticky target"
//!     rewrite — without the wrapper a bare SELECT against LOCAL would still be sent remote.
//!
//! Either way this is a control statement from the chokepoint's perspective: it already knows
//! its destination, the sticky CONNECT binding must not override it.
class PassthroughStatement : public SQLStatement {
public:
	static constexpr const StatementType TYPE = StatementType::PASSTHROUGH_STATEMENT;

public:
	PassthroughStatement(string target_p, string payload_p);
	explicit PassthroughStatement(unique_ptr<SQLStatement> local_statement_p);

	//! Remote-dispatch fields.
	string target;
	string payload;
	//! True when the marker was `CONNECT LOCAL EXECUTE …` — bind delegates to local_statement.
	bool target_is_local = false;
	//! Already-parsed inner statement for the LOCAL case. nullptr otherwise.
	unique_ptr<SQLStatement> local_statement;

protected:
	PassthroughStatement(const PassthroughStatement &other);

public:
	unique_ptr<SQLStatement> Copy() const override;
	string ToString() const override;
};

} // namespace duckdb
