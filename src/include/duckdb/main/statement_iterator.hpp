//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/statement_iterator.hpp
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
//!   StatementIterator it(ParseIterator(context, sql));
//!   while (it.Peek()) {
//!       auto stmt = it.GetStatement();
//!       if (!stmt) {
//!           continue; // a peel that preprocessing swallowed (empty expansion)
//!       }
//!       // ready-to-execute statement
//!   }
//!
class StatementIterator {
public:
	//! Wrap a lazy parse-facing stream (consumed by move). The context is inherited from the
	//! wrapped ParseIterator and must outlive this iterator.
	DUCKDB_API explicit StatementIterator(ParseIterator &&parse_iterator);
	DUCKDB_API ~StatementIterator();

	StatementIterator(const StatementIterator &) = delete;
	StatementIterator &operator=(const StatementIterator &) = delete;
	DUCKDB_API StatementIterator(StatementIterator &&) noexcept;
	// Not move-assignable: holds a ClientContext reference, which cannot be rebound.
	StatementIterator &operator=(StatementIterator &&) = delete;

	//! Returns true while more input remains (a buffered engine statement, or another parse-facing
	//! statement to pull). Parses ahead as needed but does NOT preprocess — safe as a lookahead.
	DUCKDB_API bool Peek();

	//! Grammar-free predicate: is there another statement after the current one? Never parses and
	//! never throws (see ParseIterator::HasMore), so it is safe to consult before the current
	//! statement has executed — e.g. to decide whether the current statement is the last.
	DUCKDB_API bool HasMore();

	//! Pull + preprocess the next engine-facing statement. Returns nullptr when a peel preprocesses
	//! to nothing (skip with `continue`) or when the input is exhausted (Peek would return false).
	//! Self-locking variant for callers that do not hold the context lock.
	DUCKDB_API unique_ptr<SQLStatement> GetStatement();
	//! Same, for callers that already hold the context lock.
	DUCKDB_API unique_ptr<SQLStatement> GetStatementWithLock(ClientContextLock &lock);

private:
	//! Shared body for both Get variants. `lock` is null for the self-locking path (preprocessing
	//! then acquires the lock itself) or the held lock for callers that already have it.
	unique_ptr<SQLStatement> GetStatementInternal(optional_ptr<ClientContextLock> lock);

private:
	//! The single parse-facing source this iterator preprocesses. Always constructed — the
	//! single-statement ctor forwards to ParseIterator's single-statement ctor.
	ParseIterator source;
	//! The bound context, inherited from `source`. Used for preprocessing / transaction state / locking.
	ClientContext &context;
	//! Engine-facing statements produced by preprocessing one parse-facing peel. Drained
	//! one-at-a-time across GetStatement calls before pulling + preprocessing the next peel.
	vector<unique_ptr<SQLStatement>> buffer;
	idx_t buffer_cursor = 0;
};

} // namespace duckdb
