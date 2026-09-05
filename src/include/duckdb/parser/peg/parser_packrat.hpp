//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parser_packrat.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {
class Matcher;
class ParseResult;

struct ParserPackratEntry {
	//! Whether this entry holds a memoized result
	bool valid = false;
	bool success = false;
	idx_t token_index_after = 0;
	idx_t max_token_index_seen = 0;
	optional_ptr<ParseResult> result;
};

//! Memoizes the outcome of the packrat-memoized matchers for a single parse: a dense table with one row per token
//! position, starting at the position the parse starts at, and one column per memoized matcher
class ParserPackratCache {
public:
	ParserPackratCache(idx_t start_token_index, idx_t slot_count);
	~ParserPackratCache();

	optional_ptr<const ParserPackratEntry> Lookup(const Matcher &matcher, idx_t token_index) const;
	void Store(const Matcher &matcher, idx_t token_index, ParserPackratEntry entry);

private:
	idx_t GetEntryIndex(const Matcher &matcher, idx_t token_index) const;

private:
	idx_t start_token_index;
	idx_t slot_count;
	unsafe_vector<ParserPackratEntry> entries;
};

} // namespace duckdb
