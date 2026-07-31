//===----------------------------------------------------------------------===//
//                         DuckDB
//
// collation_data.hpp
//
// Layout of the collation tables that are generated from the Unicode Consortium
// collation data by extension/icu/scripts/generate_collation_data.py.
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstdint>

namespace duckdb {
namespace collation {

//! Collation elements are stored as (primary << 32) | (secondary << 16) | tertiary,
//! where every weight is left-aligned in its field
typedef uint64_t collation_element_t;

//! Code points are mapped to a 32-bit trie value: the top two bits are the tag,
//! the remaining bits are an index into the array selected by the tag
enum CollationTag : uint32_t {
	COLLATION_TAG_NONE = 0,      // no mapping, the weight is computed
	COLLATION_TAG_SINGLE = 1,    // a single collation element
	COLLATION_TAG_EXPANSION = 2, // multiple collation elements
	COLLATION_TAG_CONTEXTS = 3   // the mapping depends on the surrounding characters
};

static constexpr uint32_t COLLATION_TAG_SHIFT = 30;
static constexpr uint32_t COLLATION_INDEX_MASK = (1U << COLLATION_TAG_SHIFT) - 1;

//! Two-stage trie: value = stage2[stage1[cp >> COLLATION_TRIE_SHIFT] + (cp & COLLATION_TRIE_MASK)]
static constexpr uint32_t COLLATION_TRIE_SHIFT = 6;
static constexpr uint32_t COLLATION_TRIE_MASK = (1U << COLLATION_TRIE_SHIFT) - 1;

enum CollationContextType : uint32_t {
	COLLATION_CONTEXT_CONTRACTION = 0, // the context follows the character
	COLLATION_CONTEXT_PREFIX = 1       // the context precedes the character, stored in reverse order
};

struct CollationExpansion {
	uint32_t ce_offset;
	uint32_t ce_count;
};

struct CollationEntry {
	uint32_t ce_offset; // the elements to use when no context matches
	uint32_t ce_count;
	uint32_t context_offset;
	uint32_t context_count;
};

struct CollationContext {
	uint32_t chars_offset;
	uint32_t chars_length;
	uint32_t type;
	uint32_t ce_offset;
	uint32_t ce_count;
};

struct CollationTable {
	const uint32_t *trie_stage1;
	const uint32_t *trie_stage2;
	const collation_element_t *ces;
	const CollationExpansion *expansions;
	const CollationEntry *entries;
	const CollationContext *contexts;
	const uint32_t *context_chars;
};

//! A locale that tailors the root collation. Only the tailored code points are stored,
//! everything else falls back to the root table.
struct CollationTailoring {
	const char *name;
	CollationTable table;
	const uint32_t *codepoints; // sorted
	const uint32_t *values;     // the trie value of each code point
	uint32_t count;
	//! Permutation of the primary lead bytes for [reorder ...], or nullptr
	const uint8_t *reorder_table;
	uint8_t strength;
	bool case_level;
	uint8_t case_first;
	bool alternate_shifted;
	bool backward_secondary;
	bool normalization;
};

extern const CollationTailoring tailorings[];
extern const uint32_t tailoring_count;

// root collation table
extern const CollationTable root_table;
extern const bool compressible_lead_byte[];
extern const uint32_t variable_top_primary;

// Han characters are ordered by radical/stroke, the order is stored as runs of code points
extern const uint32_t han_range_start[];
extern const uint16_t han_range_length[];
extern const uint32_t han_range_index[];
extern const uint32_t han_range_count;

// canonical decompositions, sorted by code point
extern const uint32_t decomposition_chars[];
extern const uint32_t decomposition_codepoint[];
extern const uint32_t decomposition_offset[];
extern const uint8_t decomposition_length[];
extern const uint32_t decomposition_count;

// canonical combining classes, stored as ranges
extern const uint32_t combining_class_start[];
extern const uint32_t combining_class_end[];
extern const uint8_t combining_class_value[];
extern const uint32_t combining_class_count;

// leading/trailing combining class of the canonical decomposition, stored as ranges
extern const uint32_t fcd_start[];
extern const uint32_t fcd_end[];
extern const uint16_t fcd_value[];
extern const uint32_t fcd_count;

} // namespace collation
} // namespace duckdb
