//===----------------------------------------------------------------------===//
//                         DuckDB
//
// collation_collator.hpp
//
// Implementation of the Unicode Collation Algorithm over the Unicode Consortium
// collation data, generating sort keys that are compatible with ICU.
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {
namespace collation {

enum class CollationStrength : uint8_t { PRIMARY = 0, SECONDARY = 1, TERTIARY = 2, QUATERNARY = 3, IDENTICAL = 4 };

enum class CaseFirst : uint8_t { DEFAULT = 0, LOWER_FIRST = 1, UPPER_FIRST = 2 };

struct CollationSettings {
	CollationStrength strength = CollationStrength::TERTIARY;
	//! Whether to add a separate case level after the secondary level
	bool case_level = false;
	CaseFirst case_first = CaseFirst::DEFAULT;
	//! Whether variable (punctuation, symbol, ...) weights are shifted to the quaternary level
	bool alternate_shifted = false;
	//! Whether secondary weights are compared backwards, as in Canadian French
	bool backward_secondary = false;
	//! Whether text that is not in canonical order is normalized before it is collated
	bool normalization = false;
};

struct CollationTailoring;

//! The buffers a collator works in, reused across the strings of a vector so that
//! generating sort keys does not allocate for every string
struct CollationBuffer {
	//! the code points of the string
	vector<uint32_t> text;
	//! the collation elements of the string
	vector<uint64_t> elements;
	//! the sort key, terminated by a null byte
	vector<uint8_t> key;
	//! the levels of the sort key that are written after the primary level
	vector<uint8_t> levels[4];
};

class Collator {
public:
	explicit Collator(CollationSettings settings_p) : settings(settings_p), tailoring(nullptr) {
	}
	//! Creates a collator for a collation name such as "de" or "fr_ca", falling back to the
	//! root collation when the locale is unknown
	explicit Collator(const string &collation);

	//! Whether a collation with this name exists
	static bool HasCollation(const string &collation);
	//! The names of all collations, in sorted order
	static vector<string> GetCollations();

	//! Writes the sort key of the given UTF-8 string into buffer.key, terminated by a null byte
	void GetSortKey(const char *data, idx_t size, CollationBuffer &buffer) const;

	//! The settings of this collator
	const CollationSettings &GetSettings() const {
		return settings;
	}
	//! Overrides the settings of the collation, used for tagged collations
	void SetSettings(CollationSettings settings_p) {
		settings = settings_p;
	}

private:
	CollationSettings settings;
	const CollationTailoring *tailoring;
};

} // namespace collation
} // namespace duckdb
