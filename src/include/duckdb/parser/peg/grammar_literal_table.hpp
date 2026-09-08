//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/peg/grammar_literal_table.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/peg/keyword_helper.hpp"

namespace duckdb {

class ParsedGrammar;
class DefaultKeywordMaps;

//! A grammar-local literal ID and the word's overlapping keyword categories.
class LiteralInfo {
public:
	static constexpr uint32_t MAX_LITERAL_ID = 0x00FFFFFF;

public:
	LiteralInfo() = default;
	explicit LiteralInfo(uint32_t literal_id) : value(literal_id) {
		D_ASSERT(literal_id <= MAX_LITERAL_ID);
	}

	uint32_t LiteralId() const {
		return value & MAX_LITERAL_ID;
	}

	bool IsKeyword() const {
		return (value >> 24) != 0;
	}

	bool HasCategory(PEGKeywordCategory category) const {
		return (value & CategoryMask(category)) != 0;
	}

	void AddCategory(PEGKeywordCategory category) {
		value |= CategoryMask(category);
	}

	bool operator==(const LiteralInfo &other) const {
		return value == other.value;
	}

private:
	static uint32_t CategoryMask(PEGKeywordCategory category) {
		if (category == PEGKeywordCategory::KEYWORD_NONE || category > PEGKeywordCategory::KEYWORD_TYPE_NAME) {
			return 0;
		}
		return uint32_t(1) << (23 + static_cast<uint8_t>(category));
	}

private:
	uint32_t value = 0;
};

//! Immutable after construction, including literals only present in keyword-category rules.
class GrammarLiteralTable {
public:
	DUCKDB_API GrammarLiteralTable(const ParsedGrammar &grammar, const DefaultKeywordMaps &keyword_maps);
	GrammarLiteralTable(const GrammarLiteralTable &) = delete;
	GrammarLiteralTable &operator=(const GrammarLiteralTable &) = delete;

	uint64_t CacheId() const {
		return cache_id;
	}

	LiteralInfo Lookup(const string &text) const {
		auto entry = literals.find(text);
		return entry == literals.end() ? LiteralInfo() : entry->second;
	}

private:
	void RegisterCategory(const case_insensitive_set_t &words, PEGKeywordCategory category);
	void Register(const string &text, PEGKeywordCategory category = PEGKeywordCategory::KEYWORD_NONE);

private:
	const uint64_t cache_id;
	case_insensitive_map_t<LiteralInfo> literals;
};

} // namespace duckdb
