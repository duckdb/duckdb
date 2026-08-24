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

class ParserPackratCache {
public:
	ParserPackratCache();
	~ParserPackratCache();

	optional_ptr<ParseResult> Match(const Matcher &matcher, MatchState &state);

private:
	unordered_map<ParserPackratKey, ParserPackratEntry, ParserPackratKeyHash> entries;
};

} // namespace duckdb
