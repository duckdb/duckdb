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
	static constexpr uint16_t MAX_LITERAL_ID = 0xFFFF;

public:
	LiteralInfo() = default;
	explicit LiteralInfo(uint16_t literal_id_p) : literal_id(literal_id_p) {
	}

	uint16_t LiteralId() const {
		return literal_id;
	}

	bool IsKeyword() const {
		return category_flags != 0;
	}

	bool HasCategory(PEGKeywordCategory category) const {
		return (category_flags & CategoryMask(category)) != 0;
	}

	void AddCategory(PEGKeywordCategory category) {
		category_flags |= CategoryMask(category);
	}

	bool operator==(const LiteralInfo &other) const {
		return literal_id == other.literal_id && category_flags == other.category_flags;
	}

private:
	static uint8_t CategoryMask(PEGKeywordCategory category) {
		if (category == PEGKeywordCategory::KEYWORD_NONE || category > PEGKeywordCategory::KEYWORD_TYPE_NAME) {
			return 0;
		}
		return uint8_t(1) << (static_cast<uint8_t>(category) - 1);
	}

private:
	uint16_t literal_id = 0;
	uint8_t category_flags = 0;
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
