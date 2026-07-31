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
	//! Decodes UTF-8 into code points, invalid bytes are decoded as U+FFFD
	static void Decode(const char *data, idx_t size, vector<uint32_t> &result);
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
