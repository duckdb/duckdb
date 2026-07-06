//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parser_packrat.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/unordered_map.hpp"

namespace duckdb {
class Matcher;
struct MatchState;
class ParseResult;

struct ParserPackratKey {
	idx_t matcher_id;
	idx_t token_index;

	bool operator==(const ParserPackratKey &other) const {
		return matcher_id == other.matcher_id && token_index == other.token_index;
	}
};

struct ParserPackratKeyHash {
	size_t operator()(const ParserPackratKey &key) const;
};

struct ParserPackratEntry {
	bool success = false;
	idx_t token_index_after = 0;
	idx_t max_token_index_seen = 0;
	optional_ptr<ParseResult> result;
};

struct ParserPackratStats {
	idx_t hits = 0;
	idx_t misses = 0;
	idx_t success_hits = 0;
	idx_t failure_hits = 0;
	idx_t stored_success = 0;
	idx_t stored_failure = 0;
};

class ParserPackratCache {
public:
	ParserPackratCache();
	~ParserPackratCache();

	static bool Enabled();
	static bool PrintStats();
	void Reset();
	optional_ptr<ParseResult> Match(const Matcher &matcher, MatchState &state);
	void Print() const;

private:
	unordered_map<ParserPackratKey, ParserPackratEntry, ParserPackratKeyHash> entries;
	ParserPackratStats stats;
};

} // namespace duckdb
