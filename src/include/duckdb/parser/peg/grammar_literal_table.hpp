//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/peg/grammar_literal_table.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/peg/literal_info.hpp"

namespace duckdb {

class ParsedGrammar;

//! Immutable after construction, including literals only present in keyword-category rules.
class GrammarLiteralTable {
public:
	DUCKDB_API GrammarLiteralTable(const ParsedGrammar &grammar, const case_insensitive_map_t<LiteralInfo> &keywords);
	GrammarLiteralTable(const GrammarLiteralTable &) = delete;
	GrammarLiteralTable &operator=(const GrammarLiteralTable &) = delete;

	uint64_t CacheId() const {
		return cache_id;
	}

	//! Calls fun(info) for every literal in the table
	template <class FUN>
	void ForEachLiteral(FUN &&fun) const {
		for (auto &entry : literals) {
			fun(entry.second);
		}
	}

	LiteralInfo Lookup(const string &text) const {
		return Lookup(text.data(), text.size());
	}
	//! Case-insensitive lookup of a text that need not be a string of its own
	LiteralInfo Lookup(const char *data, idx_t size) const {
		// most tokens that are not literals (numbers, strings, long identifiers) are rejected without hashing
		if (size == 0 || size > max_literal_length || !first_bytes[static_cast<uint8_t>(data[0])]) {
			return LiteralInfo();
		}
		if (size == 1) {
			return single_byte[static_cast<uint8_t>(data[0])];
		}
		// most identifiers share neither their first two bytes nor their length with a literal
		if (!((prefix_lengths[PrefixIndex(data[0], data[1])] >> LengthBit(size)) & 1)) {
			return LiteralInfo();
		}
		auto hash = static_cast<uint32_t>(StringUtil::CIHash(data, size));
		for (auto index = hash & slot_mask;; index = (index + 1) & slot_mask) {
			auto &slot = slots[index];
			if (!slot.entry) {
				return LiteralInfo();
			}
			auto &text = texts[slot.entry - 1];
			if (slot.hash == hash && StringUtil::CIEquals(text.data(), text.size(), data, size)) {
				return infos[slot.entry - 1];
			}
		}
	}

private:
	static constexpr idx_t PREFIX_COUNT = 1024;
	//! Slot of the first two bytes of a text: five case-insensitive bits of each
	static idx_t PrefixIndex(char first, char second) {
		return ((static_cast<uint8_t>(first) & 31U) << 5U) | (static_cast<uint8_t>(second) & 31U);
	}
	static idx_t LengthBit(idx_t size) {
		return size < 31 ? size : 31;
	}
	void Register(const string &text, keyword_categories_t categories = keyword_categories_t());
	void Index();

private:
	struct Slot {
		uint32_t hash = 0;
		//! index into texts/infos plus one; zero marks an empty slot
		uint32_t entry = 0;
	};

private:
	const uint64_t cache_id;
	idx_t max_literal_length = 0;
	case_insensitive_map_t<LiteralInfo> literals;
	//! Bytes (case-folded) that start some literal, and the literals of exactly one byte
	bool first_bytes[256] = {};
	//! Per two-byte prefix, the lengths of the literals that start with it
	uint32_t prefix_lengths[PREFIX_COUNT] = {};
	LiteralInfo single_byte[256];
	//! Open addressing index over the literals for lookups by case-insensitive hash
	vector<string> texts;
	vector<LiteralInfo> infos;
	vector<Slot> slots;
	uint32_t slot_mask = 0;
};

} // namespace duckdb
