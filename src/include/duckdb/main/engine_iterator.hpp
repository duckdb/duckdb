//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/engine_iterator.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/main/parse_iterator.hpp"

namespace duckdb {
class ClientContext;
class ClientContextLock;
class SQLStatement;

//! Iterator over the engine-facing statements of a query.
//!
//! Usage:
//!   EngineIterator it(ParseIterator(sql));
//!   while (it.Peek(context)) {
//!       auto stmt = it.GetStatement(context);
//!       if (!stmt) {
//!           continue; // a peel that preprocessing swallowed (empty expansion)
//!       }
//!       // ready-to-execute statement
//!   }
//!
//! Contract: Peek answers "is there more input?" only — it parses ahead but does NOT preprocess
//! (so it can be used as a lookahead without disturbing transaction state). GetStatement does the
//! work: it parses + preprocesses the next peel into one-or-more engine-facing statements (PRAGMA
//! reparse, MULTI_STATEMENT unpack, transaction wrapping) and returns them one at a time. A peel can
//! preprocess to zero statements, so GetStatement may return nullptr — the caller skips it with
//! `continue` and Peek reports whether more remains.
//!
//! Preprocessing needs the context lock. GetStatement(context) self-locks — for external drivers
//! that hold nothing (shell, tests). Callers that already hold the lock (Query, ParseStatements*)
//! call GetStatementWithLock(context, lock) so they don't re-lock the non-recursive context_lock.
//!
//! It always wraps exactly one ParseIterator; the two constructors differ only in how that
//! ParseIterator is sourced: from a stream (string -> ParseIterator) or from a single already-parsed
//! SQLStatement (forwarded to ParseIterator's single-statement ctor — same engine preprocessing
//! without re-parsing).
class EngineIterator {
public:
	//! Wrap a lazy parse-facing stream (consumed by move).
	DUCKDB_API explicit EngineIterator(ParseIterator &&parse_iterator);
	//! Single already-parsed statement source (still 1:N — wrapping/unpack can expand it).
	DUCKDB_API explicit EngineIterator(unique_ptr<SQLStatement> statement);
	DUCKDB_API ~EngineIterator();

	EngineIterator(const EngineIterator &) = delete;
	EngineIterator &operator=(const EngineIterator &) = delete;
	DUCKDB_API EngineIterator(EngineIterator &&) noexcept;
	DUCKDB_API EngineIterator &operator=(EngineIterator &&) noexcept;

	//! Returns true while more input remains (a buffered engine statement, or another parse-facing
	//! statement to pull). Parses ahead as needed but does NOT preprocess — safe as a lookahead.
	DUCKDB_API bool Peek(ClientContext &context);

	//! Pull + preprocess the next engine-facing statement. Returns nullptr when a peel preprocesses
	//! to nothing (skip with `continue`) or when the input is exhausted (Peek would return false).
	//! Self-locking variant for callers that do not hold the context lock.
	DUCKDB_API unique_ptr<SQLStatement> GetStatement(ClientContext &context);
	//! Same, for callers that already hold the context lock.
	DUCKDB_API unique_ptr<SQLStatement> GetStatementWithLock(ClientContext &context, ClientContextLock &lock);

private:
	//! Shared body for both Get variants. `lock` is null for the self-locking path (preprocessing
	//! then acquires the lock itself) or the held lock for callers that already have it.
	unique_ptr<SQLStatement> GetStatementInternal(ClientContext &context, optional_ptr<ClientContextLock> lock);

private:
	//! The single parse-facing source this iterator preprocesses. Always constructed — the
	//! single-statement ctor forwards to ParseIterator's single-statement ctor.
	ParseIterator source;
	//! Engine-facing statements produced by preprocessing one parse-facing peel. Drained
	//! one-at-a-time across GetStatement calls before pulling + preprocessing the next peel.
	vector<unique_ptr<SQLStatement>> buffer;
	idx_t buffer_cursor = 0;
};

} // namespace duckdb
