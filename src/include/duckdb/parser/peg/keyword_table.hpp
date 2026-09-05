//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/peg/keyword_table.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/constants.hpp"

namespace duckdb {

//! Interns every literal of a compiled grammar ('SELECT', '(', ',', ...) as a dense integer id, so that keyword
//! matching and first-set pruning compare integers instead of strings
class KeywordTable {
public:
	//! Returns the id of the keyword, registering it if it has not been seen before. Matching is case-insensitive.
	idx_t Register(const string &keyword) {
		auto entry = ids.find(keyword);
		if (entry != ids.end()) {
			return entry->second;
		}
		auto id = ids.size();
		ids.emplace(keyword, id);
		return id;
	}

	//! Returns the id of the keyword the text refers to, or DConstants::INVALID_INDEX if it is not a keyword
	idx_t Lookup(const string &text) const {
		auto entry = ids.find(text);
		if (entry == ids.end()) {
			return DConstants::INVALID_INDEX;
		}
		return entry->second;
	}

private:
	case_insensitive_map_t<idx_t> ids;
};

} // namespace duckdb
