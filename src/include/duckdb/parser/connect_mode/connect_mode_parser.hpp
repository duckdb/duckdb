//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/connect_mode/connect_mode_parser.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {
struct PEGMatcher;
struct MatcherToken;
class ParseResult;
class ListParseResult;
class ParseResultAllocator;

//! One chunk of input as seen by the connect-mode (Layer 1) parser.
//!
//!  - CONTROL:   a bare CONNECT or DISCONNECT statement (no EXECUTE clause). The caller hands the
//!    chunk's text to the main SQL parser for full handling.
//!  - EXECUTE:   a `CONNECT <name> EXECUTE <payload>` form. The caller dispatches `payload` to
//!    `<name>`'s catalog via RemoteExecute, without changing binding state.
//!  - FORBIDDEN: input that starts with CONNECT or DISCONNECT but doesn't match any well-formed
//!    control variant. The caller surfaces a clear parse error rather than forwarding silently.
//!  - RAW:       anything else. When CONNECT-ed, dispatched verbatim to the bound catalog via
//!    RemoteExecute. When not CONNECT-ed, fed to the main SQL parser as usual.
struct ConnectModeChunk {
	enum class Type : uint8_t { CONTROL, EXECUTE, FORBIDDEN, RAW };

	//! For FORBIDDEN chunks, which malformed shape did we recognize? Lets the dispatcher format a
	//! diagnostic that names the specific problem ("extra tokens after CONNECT target" vs
	//! "non-identifier after CONNECT") instead of a generic message.
	enum class ForbiddenReason : uint8_t {
		NONE,
		//! `CONNECT <ident|LOCAL|'string'> <more tokens>` — well-formed prefix plus extra junk.
		EXTRA_TOKENS_AFTER_CONNECT_TARGET,
		//! `CONNECT <something that isn't an identifier / LOCAL / string-literal>` — e.g.
		//! `CONNECT 42`.
		INVALID_CONNECT_TARGET,
		//! `CONNECT` alone with no target whatsoever.
		MISSING_CONNECT_TARGET,
		//! `CONNECT EXECUTE <sql>` — the target name between CONNECT and EXECUTE is missing.
		//! Caught separately so the message can hint at the correct shape.
		CONNECT_EXECUTE_MISSING_TARGET,
		//! `DISCONNECT <anything>` — DISCONNECT takes no arguments.
		EXTRA_TOKENS_AFTER_DISCONNECT,
	};

	Type type;
	ForbiddenReason forbidden_reason = ForbiddenReason::NONE;
	//! Slice of the original input that this chunk corresponds to (no surrounding `;`).
	string text;

	//! Populated only for EXECUTE chunks.
	string target;
	string payload;
};

//! Layer 1 parser: identifies CONNECT / DISCONNECT / `CONNECT name EXECUTE <raw>` statements and
//! captures everything else as opaque Raw chunks (string/comment-aware).
//!
//! Iterator-style by design: tokenize once upfront, then peel one chunk at a time. The caller
//! interleaves Next() with execution so that state changes from one chunk (binding/unbinding) are
//! visible when classifying the next.
class ConnectModeParser {
public:
	explicit ConnectModeParser(const string &sql);
	~ConnectModeParser();

	//! Return the next chunk, or nullopt if input is exhausted.
	bool TryGetNext(ConnectModeChunk &out_chunk);

	//! Convenience: collect all remaining chunks into a vector.
	vector<ConnectModeChunk> AllRemaining();

private:
	void ClassifyChunk(ParseResult &chunk_node, ConnectModeChunk &out);

private:
	//! Owned copy of the input — slicing inside ClassifyChunk reads from this after the parse phase
	//! has finished, so capturing by reference would dangle when the caller passed a temporary.
	string sql;
	//! Compiled connect-mode grammar matcher. Cached at first use, shared by all instances.
	shared_ptr<PEGMatcher> matcher;
	//! Tokenized input (filled at construction).
	vector<MatcherToken> tokens;
	//! Owns the parse result tree (allocated during MatchParseResult).
	unique_ptr<ParseResultAllocator> result_allocator;
	//! Root of the parse tree (the Program node). Null when the input is empty/whitespace-only.
	optional_ptr<ListParseResult> program;
	//! Index of the next top-level chunk to emit from `program`.
	idx_t cursor = 0;
};

} // namespace duckdb
