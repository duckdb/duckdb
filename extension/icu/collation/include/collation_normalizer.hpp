//===----------------------------------------------------------------------===//
//                         DuckDB
//
// collation_normalizer.hpp
//
// Canonical decomposition (NFD) of the text that is collated.
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {
namespace collation {

class Normalizer {
public:
	//! What the decoded text contains, so that the passes over it can be skipped
	enum TextFlags : uint32_t {
		TEXT_HAS_HANGUL = 1, // a Hangul syllable, which is always decomposed
		TEXT_HAS_MARKS = 2   // a character that can carry a combining class
	};

	//! Decodes UTF-8 into code points, invalid bytes are decoded as U+FFFD
	static uint32_t Decode(const char *data, idx_t size, vector<uint32_t> &result);
	//! Whether the combining marks in the text are in canonical order
	static bool IsFCD(const vector<uint32_t> &text);
	//! Replaces the text with its canonical decomposition (NFD)
	static void Decompose(vector<uint32_t> &text);
	//! Decomposes Hangul syllables into jamo, which are always collated separately
	static void DecomposeHangul(vector<uint32_t> &text);
	//! The canonical combining class of a code point
	static uint8_t CombiningClass(uint32_t codepoint);
};

} // namespace collation
} // namespace duckdb
