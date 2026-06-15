//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/engine_iterator.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/main/parse_iterator.hpp"

namespace duckdb {
class ClientContext;
class SQLStatement;

//! Iterator over the engine-facing statements of a query.
//!
//! Usage (over a parse stream):
//!   EngineIterator it(ParseIterator(sql));
//!   while (it.Peek(context)) {
//!       auto stmt = it.GetStatement();
//!       // ready-to-execute statement
//!   }
//!
//! This is the upper of the two statement iterators. Its atom is "preprocess one parse-facing
//! SQLStatement into the one-or-more engine-facing statements the engine actually runs" (PRAGMA
//! reparse, MULTI_STATEMENT unpack, transaction wrapping) — a 1:N step. It always wraps exactly one
//! ParseIterator; the two constructors differ only in how that ParseIterator is sourced:
//!   - from a ParseIterator: lazy stream source (the common case; `string -> EngineIterator` is
//!     sugar for ParseIterator -> EngineIterator).
//!   - from a single SQLStatement: forwards to ParseIterator's single-statement ctor, for callers
//!     that already hold a parsed statement (prepared-statement path, extension-produced
//!     statements) and want identical engine-level preprocessing without re-parsing.
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

	//! Returns true if an engine-facing statement is currently available (parsing + preprocessing
	//! on demand). Returns false when exhausted.
	DUCKDB_API bool Peek(ClientContext &context);

	//! Returns the next buffered engine-facing statement and clears the buffer. Returns nullptr if
	//! none is buffered — the caller should call Peek(context) first.
	DUCKDB_API unique_ptr<SQLStatement> GetStatement();

private:
	//! The single parse-facing source this iterator preprocesses. Always constructed — the
	//! single-statement ctor forwards to ParseIterator's single-statement ctor.
	ParseIterator source;
	//! Engine-facing statements produced by preprocessing one parse-facing statement. Drained
	//! one-at-a-time across Peek calls before pulling the next parse-facing statement.
	vector<unique_ptr<SQLStatement>> buffer;
	idx_t buffer_cursor = 0;
	//! Single-statement buffer holding the result of the most recent Peek. Cleared by GetStatement.
	unique_ptr<SQLStatement> current_statement;
	//! Once the source is drained and the buffer empty, we stay exhausted.
	bool exhausted = false;
};

} // namespace duckdb
