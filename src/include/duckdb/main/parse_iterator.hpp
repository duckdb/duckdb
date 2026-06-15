//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/parse_iterator.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {
class ClientContext;
class Parser;
class SQLStatement;
struct MatcherToken;

//! Iterator over the parse-facing statements of a multi-statement query.
//!
//! Usage:
//!   ParseIterator it(sql);
//!   while (it.Peek(context)) {
//!       auto stmt = it.GetStatement();
//!       // raw, just-parsed statement
//!   }
//!
//! Separator-only stretches (e.g. ";;;") are transparently skipped inside Peek — the caller
//! always sees either a real statement or a clean exhaustion.
class ParseIterator {
public:
	//! Peel parse-facing statements out of a SQL string (PEG / parser_override).
	DUCKDB_API explicit ParseIterator(const string &sql);
	DUCKDB_API ~ParseIterator();

	ParseIterator(const ParseIterator &) = delete;
	ParseIterator &operator=(const ParseIterator &) = delete;
	DUCKDB_API ParseIterator(ParseIterator &&) noexcept;
	DUCKDB_API ParseIterator &operator=(ParseIterator &&) noexcept;

	//! Returns true if a statement is currently available (after parsing as needed). Returns
	//! false when the iterator is exhausted. Non-const: parses on demand and buffers the result.
	DUCKDB_API bool Peek(ClientContext &context);

	//! Returns the next buffered statement and clears the buffer. Returns nullptr if no
	//! statement is buffered — the caller should call Peek(context) first.
	DUCKDB_API unique_ptr<SQLStatement> GetStatement();

private:
	string sql;
	//! Parser instance kept alive across Peek calls so its PEG matcher / transformer caches
	//! stay warm. Constructed lazily on the first Peek.
	unique_ptr<Parser> parser;
	//! Tokenized view of `sql`. Populated once on the first Peek and walked thereafter via
	//! `token_cursor`, avoiding O(N²) re-tokenization across N statements.
	unique_ptr<vector<MatcherToken>> tokens;
	//! Index into `tokens` at which the next match starts.
	idx_t token_cursor = 0;
	//! Single-statement buffer holding the result of the most recent Peek. Cleared by
	//! GetStatement.
	unique_ptr<SQLStatement> current_statement;
	//! Once Peek determines there are no more statements (cursor past end of tokens), we stay
	//! exhausted; subsequent Peek calls return false without re-invoking the parser.
	bool exhausted = false;
	//! Statements produced by a successful `parser_override` extension; if non-empty the
	//! iterator yields these in order instead of running the PEG parser at all. Populated on
	//! the first Peek when an extension claims the query.
	unique_ptr<vector<unique_ptr<SQLStatement>>> overridden_statements;
	//! Cursor into `overridden_statements`.
	idx_t override_cursor = 0;
	//! True once we've consulted parser_override extensions for this query.
	bool override_resolved = false;
};

} // namespace duckdb
