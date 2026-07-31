#include "collation_collator.hpp"

#include "collation_data.hpp"
#include "collation_normalizer.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {
namespace collation {

//===--------------------------------------------------------------------===//
// Weight constants
//===--------------------------------------------------------------------===//
// A collation element holds a 32-bit primary weight and 16-bit secondary and
// tertiary weights, every weight is a left-aligned sequence of bytes.

static constexpr uint8_t LEVEL_SEPARATOR_BYTE = 0x01;
static constexpr uint8_t MERGE_SEPARATOR_BYTE = 0x02;
static constexpr uint32_t MERGE_SEPARATOR_PRIMARY = 0x02000000;
static constexpr uint8_t PRIMARY_COMPRESSION_LOW_BYTE = 0x03;
static constexpr uint8_t PRIMARY_COMPRESSION_HIGH_BYTE = 0xFF;

static constexpr uint32_t COMMON_WEIGHT = 0x0500;
//! The primary weight of the sentinel element that terminates the levels
static constexpr uint32_t NO_CE_PRIMARY = 1;
static constexpr uint32_t NO_CE_WEIGHT = 0x0100;
static constexpr collation_element_t NO_CE = (static_cast<collation_element_t>(NO_CE_PRIMARY) << 32) |
                                             (static_cast<collation_element_t>(NO_CE_WEIGHT) << 16) | NO_CE_WEIGHT;

static constexpr uint32_t CASE_MASK = 0xC000;
static constexpr uint32_t ONLY_TERTIARY_MASK = 0x3F3F;
static constexpr uint32_t CASE_AND_TERTIARY_MASK = CASE_MASK | ONLY_TERTIARY_MASK;

// runs of common weights are compressed into a single byte per level
static constexpr uint32_t SEC_COMMON_LOW = 0x05;
static constexpr uint32_t SEC_COMMON_MIDDLE = SEC_COMMON_LOW + 0x20;
static constexpr uint32_t SEC_COMMON_HIGH = SEC_COMMON_LOW + 0x40;
static constexpr int32_t SEC_COMMON_MAX_COUNT = 0x21;

static constexpr uint32_t CASE_LOWER_FIRST_COMMON_LOW = 1;
static constexpr uint32_t CASE_LOWER_FIRST_COMMON_MIDDLE = 7;
static constexpr uint32_t CASE_LOWER_FIRST_COMMON_HIGH = 13;
static constexpr int32_t CASE_LOWER_FIRST_COMMON_MAX_COUNT = 7;

static constexpr uint32_t CASE_UPPER_FIRST_COMMON_LOW = 3;
static constexpr int32_t CASE_UPPER_FIRST_COMMON_MAX_COUNT = 13;

static constexpr uint32_t TER_ONLY_COMMON_LOW = 0x05;
static constexpr uint32_t TER_ONLY_COMMON_MIDDLE = TER_ONLY_COMMON_LOW + 0x60;
static constexpr uint32_t TER_ONLY_COMMON_HIGH = TER_ONLY_COMMON_LOW + 0xC0;
static constexpr int32_t TER_ONLY_COMMON_MAX_COUNT = 0x61;

static constexpr uint32_t TER_LOWER_FIRST_COMMON_LOW = 0x05;
static constexpr uint32_t TER_LOWER_FIRST_COMMON_MIDDLE = TER_LOWER_FIRST_COMMON_LOW + 0x20;
static constexpr uint32_t TER_LOWER_FIRST_COMMON_HIGH = TER_LOWER_FIRST_COMMON_LOW + 0x40;
static constexpr int32_t TER_LOWER_FIRST_COMMON_MAX_COUNT = 0x21;

static constexpr uint32_t TER_UPPER_FIRST_COMMON_LOW = 0x05 + 0x80;
static constexpr uint32_t TER_UPPER_FIRST_COMMON_MIDDLE = TER_UPPER_FIRST_COMMON_LOW + 0x20;
static constexpr uint32_t TER_UPPER_FIRST_COMMON_HIGH = TER_UPPER_FIRST_COMMON_LOW + 0x40;
static constexpr int32_t TER_UPPER_FIRST_COMMON_MAX_COUNT = 0x21;

static constexpr uint32_t QUAT_COMMON_LOW = 0x1C;
static constexpr uint32_t QUAT_COMMON_MIDDLE = QUAT_COMMON_LOW + 0x70;
static constexpr uint32_t QUAT_COMMON_HIGH = QUAT_COMMON_LOW + 0xE0;
static constexpr int32_t QUAT_COMMON_MAX_COUNT = 0x71;
static constexpr uint32_t QUAT_SHIFTED_LIMIT_BYTE = QUAT_COMMON_LOW - 1;

//! Han characters are ordered by radical/stroke, their primary weights are assigned
//! sequentially from the Han primary range
static constexpr uint32_t HAN_FIRST_LEAD_BYTE = 0xFA;
static constexpr uint32_t HAN_THIRD_BYTE_COUNT = 127;
static constexpr uint32_t HAN_PER_LEAD_BYTE = HAN_THIRD_BYTE_COUNT * 254;
//! Code points without a mapping get a primary weight computed from the code point
static constexpr uint32_t UNASSIGNED_IMPLICIT_LEAD_BYTE = 0xFE;

//===--------------------------------------------------------------------===//
// Collation element lookup
//===--------------------------------------------------------------------===//

static collation_element_t MakeElement(uint32_t primary, uint32_t secondary, uint32_t tertiary) {
	return (static_cast<collation_element_t>(primary) << 32) | (static_cast<collation_element_t>(secondary) << 16) |
	       tertiary;
}

//! Returns the position of a Han character in radical/stroke order, or false if it is not a Han character
static bool GetHanIndex(uint32_t codepoint, uint32_t &result) {
	uint32_t lower = 0;
	uint32_t upper = han_range_count;
	while (lower < upper) {
		auto middle = (lower + upper) / 2;
		auto start = han_range_start[middle];
		if (codepoint < start) {
			upper = middle;
		} else if (codepoint >= start + han_range_length[middle]) {
			lower = middle + 1;
		} else {
			result = han_range_index[middle] + (codepoint - start);
			return true;
		}
	}
	return false;
}

//! The collation element of a code point that has no mapping in the table
static collation_element_t ImplicitElement(uint32_t codepoint) {
	uint32_t index;
	uint32_t primary;
	if (GetHanIndex(codepoint, index)) {
		auto lead = HAN_FIRST_LEAD_BYTE + index / HAN_PER_LEAD_BYTE;
		auto rest = index % HAN_PER_LEAD_BYTE;
		primary = (lead << 24) | ((0x02 + rest / HAN_THIRD_BYTE_COUNT) << 16) |
		          ((0x02 + 2 * (rest % HAN_THIRD_BYTE_COUNT)) << 8);
	} else {
		auto value = codepoint + 1;
		primary = 2 + (value % 18) * 14;
		value /= 18;
		primary |= (2 + (value % 254)) << 8;
		value /= 254;
		primary |= (4 + (value % 251)) << 16;
		primary |= UNASSIGNED_IMPLICIT_LEAD_BYTE << 24;
	}
	return MakeElement(primary, COMMON_WEIGHT, COMMON_WEIGHT);
}

static uint32_t LookupValue(const CollationTable &table, uint32_t codepoint) {
	auto block = table.trie_stage1[codepoint >> COLLATION_TRIE_SHIFT];
	return table.trie_stage2[block + (codepoint & COLLATION_TRIE_MASK)];
}

//===--------------------------------------------------------------------===//
// Collation element generation
//===--------------------------------------------------------------------===//

//! Looks up the trie value of a code point in a tailoring, returns false when the
//! locale does not tailor it
static bool LookupTailoringValue(const CollationTailoring &tailoring, uint32_t codepoint, uint32_t &result) {
	uint32_t lower = 0;
	uint32_t upper = tailoring.count;
	while (lower < upper) {
		auto middle = (lower + upper) / 2;
		auto value = tailoring.codepoints[middle];
		if (codepoint < value) {
			upper = middle;
		} else if (codepoint > value) {
			lower = middle + 1;
		} else {
			result = tailoring.values[middle];
			return true;
		}
	}
	return false;
}

class ElementGenerator {
public:
	ElementGenerator(const CollationTailoring *tailoring_p, vector<uint32_t> &text_p)
	    : table(&root_table), tailoring(tailoring_p), text(text_p) {
	}

	void Generate(vector<collation_element_t> &result);

private:
	//! Appends the elements at [offset, offset + count) of the table
	void AppendElements(uint32_t offset, uint32_t count, vector<collation_element_t> &result);
	//! Matches the contexts of an entry at the current position, returns true if one matched
	bool MatchContext(const CollationEntry &entry, idx_t position, uint32_t &ce_offset, uint32_t &ce_count,
	                  idx_t &consumed, vector<idx_t> &skipped);
	bool MatchContraction(const CollationContext &context, idx_t position, idx_t &consumed, vector<idx_t> &skipped);
	bool MatchPrefix(const CollationContext &context, idx_t position);

private:
	//! the table the code point that is being processed comes from
	const CollationTable *table;
	const CollationTailoring *tailoring;
	vector<uint32_t> &text;
};

void ElementGenerator::AppendElements(uint32_t offset, uint32_t count, vector<collation_element_t> &result) {
	for (uint32_t i = 0; i < count; i++) {
		auto element = table->ces[offset + i];
		if (element != 0) {
			// completely ignorable elements are not emitted
			result.push_back(element);
		}
	}
}

bool ElementGenerator::MatchPrefix(const CollationContext &context, idx_t position) {
	if (position < context.chars_length) {
		return false;
	}
	// the context is stored in reverse order, closest character first
	for (uint32_t i = 0; i < context.chars_length; i++) {
		if (text[position - 1 - i] != table->context_chars[context.chars_offset + i]) {
			return false;
		}
	}
	return true;
}

bool ElementGenerator::MatchContraction(const CollationContext &context, idx_t position, idx_t &consumed,
                                        vector<idx_t> &skipped) {
	auto chars = table->context_chars + context.chars_offset;
	// a contraction can skip over combining marks that do not block the character it
	// continues with, which is the case when their combining class is lower
	idx_t pos = position + 1;
	uint32_t matched = 0;
	bool discontiguous = false;
	skipped.clear();
	while (matched < context.chars_length && pos < text.size()) {
		auto combining_class = Normalizer::CombiningClass(text[pos]);
		if (text[pos] == chars[matched]) {
			if (discontiguous && combining_class == 0) {
				// only combining marks can be moved out of order
				return false;
			}
			if (discontiguous) {
				skipped.push_back(pos);
			}
			matched++;
			pos++;
			continue;
		}
		if (combining_class == 0 || combining_class >= Normalizer::CombiningClass(chars[matched])) {
			// the character blocks the rest of the contraction
			return false;
		}
		discontiguous = true;
		pos++;
	}
	if (matched < context.chars_length) {
		return false;
	}
	// everything up to the first skipped character is consumed directly
	consumed = skipped.empty() ? context.chars_length + 1 : 1;
	return true;
}

bool ElementGenerator::MatchContext(const CollationEntry &entry, idx_t position, uint32_t &ce_offset,
                                    uint32_t &ce_count, idx_t &consumed, vector<idx_t> &skipped) {
	vector<idx_t> candidate_skipped;
	for (uint32_t i = 0; i < entry.context_count; i++) {
		auto &context = table->contexts[entry.context_offset + i];
		if (context.type == COLLATION_CONTEXT_PREFIX) {
			if (!MatchPrefix(context, position)) {
				continue;
			}
			consumed = 1;
			skipped.clear();
		} else {
			if (!MatchContraction(context, position, consumed, candidate_skipped)) {
				continue;
			}
			skipped = candidate_skipped;
		}
		ce_offset = context.ce_offset;
		ce_count = context.ce_count;
		return true;
	}
	return false;
}

void ElementGenerator::Generate(vector<collation_element_t> &result) {
	vector<idx_t> skipped;
	idx_t position = 0;
	while (position < text.size()) {
		auto codepoint = text[position];
		uint32_t value;
		if (tailoring && LookupTailoringValue(*tailoring, codepoint, value)) {
			table = &tailoring->table;
		} else {
			table = &root_table;
			value = LookupValue(root_table, codepoint);
		}
		auto tag = value >> COLLATION_TAG_SHIFT;
		auto index = value & COLLATION_INDEX_MASK;
		idx_t consumed = 1;
		if (tag == COLLATION_TAG_CONTEXTS) {
			auto &entry = table->entries[index];
			uint32_t ce_offset;
			uint32_t ce_count;
			if (MatchContext(entry, position, ce_offset, ce_count, consumed, skipped)) {
				AppendElements(ce_offset, ce_count, result);
				// characters that were skipped by a discontiguous contraction are removed
				// from the text, the remaining characters keep their relative order
				for (idx_t i = skipped.size(); i > 0; i--) {
					text.erase(text.begin() + static_cast<int64_t>(skipped[i - 1]));
				}
			} else if (entry.ce_count > 0) {
				AppendElements(entry.ce_offset, entry.ce_count, result);
			} else {
				result.push_back(ImplicitElement(codepoint));
			}
		} else if (tag == COLLATION_TAG_SINGLE) {
			auto element = table->ces[index];
			if (element != 0) {
				result.push_back(element);
			}
		} else if (tag == COLLATION_TAG_EXPANSION) {
			auto &expansion = table->expansions[index];
			AppendElements(expansion.ce_offset, expansion.ce_count, result);
		} else {
			result.push_back(ImplicitElement(codepoint));
		}
		position += consumed;
	}
	result.push_back(NO_CE);
}

//===--------------------------------------------------------------------===//
// Sort key generation
//===--------------------------------------------------------------------===//

//! A single level of the sort key, the levels are concatenated once all elements are processed
class SortKeyLevel {
public:
	explicit SortKeyLevel(vector<uint8_t> &bytes_p) : bytes(bytes_p) {
		bytes.clear();
	}
	void AppendByte(uint32_t byte) {
		bytes.push_back(static_cast<uint8_t>(byte));
	}
	//! Appends a 16-bit weight, dropping the trailing zero byte
	void AppendWeight16(uint32_t weight) {
		bytes.push_back(static_cast<uint8_t>(weight >> 8));
		if ((weight & 0xFF) != 0) {
			bytes.push_back(static_cast<uint8_t>(weight));
		}
	}
	//! Appends a 16-bit weight in reverse byte order, used by the backwards secondary level
	void AppendReverseWeight16(uint32_t weight) {
		if ((weight & 0xFF) != 0) {
			bytes.push_back(static_cast<uint8_t>(weight));
		}
		bytes.push_back(static_cast<uint8_t>(weight >> 8));
	}
	//! Appends a 32-bit weight, dropping the trailing zero bytes
	void AppendWeight32(uint32_t weight) {
		for (int32_t shift = 24; shift >= 0; shift -= 8) {
			auto byte = static_cast<uint8_t>(weight >> shift);
			if (byte == 0) {
				break;
			}
			bytes.push_back(byte);
		}
	}
	bool IsEmpty() const {
		return bytes.empty();
	}
	idx_t Size() const {
		return bytes.size();
	}
	uint8_t &operator[](idx_t index) {
		return bytes[index];
	}
	//! Appends everything but the trailing level terminator to the sort key
	void AppendTo(vector<uint8_t> &result) const {
		result.insert(result.end(), bytes.begin(), bytes.end() - 1);
	}

private:
	vector<uint8_t> &bytes;
};

//! The levels that are written for a given strength
static uint32_t GetLevelMask(const CollationSettings &settings) {
	uint32_t levels;
	switch (settings.strength) {
	case CollationStrength::PRIMARY:
		levels = 0x02;
		break;
	case CollationStrength::SECONDARY:
		levels = 0x06;
		break;
	case CollationStrength::TERTIARY:
		levels = 0x16;
		break;
	default:
		levels = 0x36;
		break;
	}
	if (settings.case_level) {
		levels |= 0x08;
	}
	return levels;
}

static constexpr uint32_t PRIMARY_LEVEL_FLAG = 0x02;
static constexpr uint32_t SECONDARY_LEVEL_FLAG = 0x04;
static constexpr uint32_t CASE_LEVEL_FLAG = 0x08;
static constexpr uint32_t TERTIARY_LEVEL_FLAG = 0x10;
static constexpr uint32_t QUATERNARY_LEVEL_FLAG = 0x20;

static void WriteSortKey(const vector<collation_element_t> &elements, const CollationSettings &settings,
                         const uint8_t *reorder_table, CollationBuffer &buffer) {
	auto &result = buffer.key;
	auto levels = GetLevelMask(settings);
	auto upper_first = settings.case_first == CaseFirst::UPPER_FIRST;
	// the case bits are only kept in the tertiary weight when caseFirst is set without a case level
	auto tertiary_mask = (settings.case_first != CaseFirst::DEFAULT && !settings.case_level) ? CASE_AND_TERTIARY_MASK
	                                                                                         : ONLY_TERTIARY_MASK;
	// +1 so that primary ignorables test out early
	uint32_t variable_top = settings.alternate_shifted ? variable_top_primary + 1 : 0;

	SortKeyLevel secondaries(buffer.levels[0]);
	SortKeyLevel cases(buffer.levels[1]);
	SortKeyLevel tertiaries(buffer.levels[2]);
	SortKeyLevel quaternaries(buffer.levels[3]);

	uint32_t previous_primary = 0; // 0 means no primary compression is in progress
	int32_t common_cases = 0;
	int32_t common_secondaries = 0;
	int32_t common_tertiaries = 0;
	int32_t common_quaternaries = 0;
	uint32_t previous_secondary = 0;
	idx_t secondary_segment_start = 0;

	idx_t position = 0;
	while (position < elements.size()) {
		auto element = elements[position++];
		auto primary = static_cast<uint32_t>(element >> 32);
		if (primary < variable_top && primary > MERGE_SEPARATOR_PRIMARY) {
			// a variable element, shift it to the quaternary level and ignore
			// the primary ignorables that follow it
			if (common_quaternaries != 0) {
				common_quaternaries--;
				while (common_quaternaries >= QUAT_COMMON_MAX_COUNT) {
					quaternaries.AppendByte(QUAT_COMMON_MIDDLE);
					common_quaternaries -= QUAT_COMMON_MAX_COUNT;
				}
				quaternaries.AppendByte(QUAT_COMMON_LOW + static_cast<uint32_t>(common_quaternaries));
				common_quaternaries = 0;
			}
			do {
				if ((levels & QUATERNARY_LEVEL_FLAG) != 0) {
					if (reorder_table) {
						primary = (static_cast<uint32_t>(reorder_table[primary >> 24]) << 24) | (primary & 0x00FFFFFF);
					}
					if ((primary >> 24) >= QUAT_SHIFTED_LIMIT_BYTE) {
						// keep shifted primaries out of the common compression range
						quaternaries.AppendByte(QUAT_SHIFTED_LIMIT_BYTE);
					}
					quaternaries.AppendWeight32(primary);
				}
				do {
					element = elements[position++];
					primary = static_cast<uint32_t>(element >> 32);
				} while (primary == 0);
			} while (primary < variable_top && primary > MERGE_SEPARATOR_PRIMARY);
		}
		if (primary > NO_CE_PRIMARY && (levels & PRIMARY_LEVEL_FLAG) != 0) {
			// the un-reordered primary determines whether the weight can be compressed
			auto compressible = compressible_lead_byte[primary >> 24];
			if (reorder_table) {
				primary = (static_cast<uint32_t>(reorder_table[primary >> 24]) << 24) | (primary & 0x00FFFFFF);
			}
			auto lead = primary >> 24;
			if (!compressible || lead != (previous_primary >> 24)) {
				if (previous_primary != 0) {
					if (primary < previous_primary) {
						// no compression terminator at the end of the level
						if (lead > MERGE_SEPARATOR_BYTE) {
							result.push_back(PRIMARY_COMPRESSION_LOW_BYTE);
						}
					} else {
						result.push_back(PRIMARY_COMPRESSION_HIGH_BYTE);
					}
				}
				result.push_back(static_cast<uint8_t>(lead));
				previous_primary = compressible ? primary : 0;
			}
			auto second = static_cast<uint8_t>(primary >> 16);
			if (second != 0) {
				result.push_back(second);
				auto third = static_cast<uint8_t>(primary >> 8);
				if (third != 0) {
					result.push_back(third);
					auto fourth = static_cast<uint8_t>(primary);
					if (fourth != 0) {
						result.push_back(fourth);
					}
				}
			}
		}

		auto lower = static_cast<uint32_t>(element);
		if (lower == 0) {
			// completely ignorable
			continue;
		}

		if ((levels & SECONDARY_LEVEL_FLAG) != 0) {
			auto secondary = lower >> 16;
			if (secondary == 0) {
				// secondary ignorable
			} else if (secondary == COMMON_WEIGHT &&
			           (!settings.backward_secondary || primary != MERGE_SEPARATOR_PRIMARY)) {
				common_secondaries++;
			} else if (!settings.backward_secondary) {
				if (common_secondaries != 0) {
					common_secondaries--;
					while (common_secondaries >= SEC_COMMON_MAX_COUNT) {
						secondaries.AppendByte(SEC_COMMON_MIDDLE);
						common_secondaries -= SEC_COMMON_MAX_COUNT;
					}
					auto byte = secondary < COMMON_WEIGHT ? SEC_COMMON_LOW + static_cast<uint32_t>(common_secondaries)
					                                      : SEC_COMMON_HIGH - static_cast<uint32_t>(common_secondaries);
					secondaries.AppendByte(byte);
					common_secondaries = 0;
				}
				secondaries.AppendWeight16(secondary);
			} else {
				if (common_secondaries != 0) {
					common_secondaries--;
					// the weights are appended in reverse, the level is reversed again below
					auto remainder = common_secondaries % SEC_COMMON_MAX_COUNT;
					auto byte = previous_secondary < COMMON_WEIGHT ? SEC_COMMON_LOW + static_cast<uint32_t>(remainder)
					                                               : SEC_COMMON_HIGH - static_cast<uint32_t>(remainder);
					secondaries.AppendByte(byte);
					common_secondaries -= remainder;
					while (common_secondaries > 0) {
						secondaries.AppendByte(SEC_COMMON_MIDDLE);
						common_secondaries -= SEC_COMMON_MAX_COUNT;
					}
				}
				if (primary > 0 && primary <= MERGE_SEPARATOR_PRIMARY) {
					// backwards secondary weights are compared within segments
					// that are separated by the merge separator
					auto last = secondaries.Size() - 1;
					if (secondary_segment_start < last) {
						for (idx_t left = secondary_segment_start, right = last; left < right; left++, right--) {
							std::swap(secondaries[left], secondaries[right]);
						}
					}
					secondaries.AppendByte(primary == NO_CE_PRIMARY ? LEVEL_SEPARATOR_BYTE : MERGE_SEPARATOR_BYTE);
					previous_secondary = 0;
					secondary_segment_start = secondaries.Size();
				} else {
					secondaries.AppendReverseWeight16(secondary);
					previous_secondary = secondary;
				}
			}
		}

		if ((levels & CASE_LEVEL_FLAG) != 0) {
			auto ignorable = settings.strength == CollationStrength::PRIMARY ? primary == 0 : lower <= 0xFFFF;
			if (!ignorable) {
				// the case bits and the tertiary lead byte
				auto value = (lower >> 8) & 0xFF;
				if ((value & 0xC0) == 0 && value > LEVEL_SEPARATOR_BYTE) {
					common_cases++;
				} else {
					if (!upper_first) {
						// lowerFirst: common weights compress to nibbles 1..7..13, mixed is 14, upper is 15
						if (common_cases != 0 && (value > LEVEL_SEPARATOR_BYTE || !cases.IsEmpty())) {
							common_cases--;
							while (common_cases >= CASE_LOWER_FIRST_COMMON_MAX_COUNT) {
								cases.AppendByte(CASE_LOWER_FIRST_COMMON_MIDDLE << 4);
								common_cases -= CASE_LOWER_FIRST_COMMON_MAX_COUNT;
							}
							auto byte = value <= LEVEL_SEPARATOR_BYTE
							                ? CASE_LOWER_FIRST_COMMON_LOW + static_cast<uint32_t>(common_cases)
							                : CASE_LOWER_FIRST_COMMON_HIGH - static_cast<uint32_t>(common_cases);
							cases.AppendByte(byte << 4);
							common_cases = 0;
						}
						if (value > LEVEL_SEPARATOR_BYTE) {
							value = (CASE_LOWER_FIRST_COMMON_HIGH + (value >> 6)) << 4;
						}
					} else {
						// upperFirst: common weights compress to nibbles 3..15, mixed is 2, upper is 1
						if (common_cases != 0) {
							common_cases--;
							while (common_cases >= CASE_UPPER_FIRST_COMMON_MAX_COUNT) {
								cases.AppendByte(CASE_UPPER_FIRST_COMMON_LOW << 4);
								common_cases -= CASE_UPPER_FIRST_COMMON_MAX_COUNT;
							}
							cases.AppendByte((CASE_UPPER_FIRST_COMMON_LOW + static_cast<uint32_t>(common_cases)) << 4);
							common_cases = 0;
						}
						if (value > LEVEL_SEPARATOR_BYTE) {
							value = (CASE_UPPER_FIRST_COMMON_LOW - (value >> 6)) << 4;
						}
					}
					cases.AppendByte(value);
				}
			}
		}

		if ((levels & TERTIARY_LEVEL_FLAG) != 0) {
			auto tertiary = lower & tertiary_mask;
			if (tertiary == COMMON_WEIGHT) {
				common_tertiaries++;
			} else if ((tertiary_mask & 0x8000) == 0) {
				// tertiary weights without case bits, lead bytes 06..3F move to C6..FF
				if (common_tertiaries != 0) {
					common_tertiaries--;
					while (common_tertiaries >= TER_ONLY_COMMON_MAX_COUNT) {
						tertiaries.AppendByte(TER_ONLY_COMMON_MIDDLE);
						common_tertiaries -= TER_ONLY_COMMON_MAX_COUNT;
					}
					auto byte = tertiary < COMMON_WEIGHT
					                ? TER_ONLY_COMMON_LOW + static_cast<uint32_t>(common_tertiaries)
					                : TER_ONLY_COMMON_HIGH - static_cast<uint32_t>(common_tertiaries);
					tertiaries.AppendByte(byte);
					common_tertiaries = 0;
				}
				if (tertiary > COMMON_WEIGHT) {
					tertiary += 0xC000;
				}
				tertiaries.AppendWeight16(tertiary);
			} else if (!upper_first) {
				// tertiary weights with caseFirst=lowerFirst, lead bytes 06..BF move to 46..FF
				if (common_tertiaries != 0) {
					common_tertiaries--;
					while (common_tertiaries >= TER_LOWER_FIRST_COMMON_MAX_COUNT) {
						tertiaries.AppendByte(TER_LOWER_FIRST_COMMON_MIDDLE);
						common_tertiaries -= TER_LOWER_FIRST_COMMON_MAX_COUNT;
					}
					auto byte = tertiary < COMMON_WEIGHT
					                ? TER_LOWER_FIRST_COMMON_LOW + static_cast<uint32_t>(common_tertiaries)
					                : TER_LOWER_FIRST_COMMON_HIGH - static_cast<uint32_t>(common_tertiaries);
					tertiaries.AppendByte(byte);
					common_tertiaries = 0;
				}
				if (tertiary > COMMON_WEIGHT) {
					tertiary += 0x4000;
				}
				tertiaries.AppendWeight16(tertiary);
			} else {
				// tertiary weights with caseFirst=upperFirst
				if (tertiary <= NO_CE_WEIGHT) {
					// separators are unchanged
				} else if (lower > 0xFFFF) {
					// invert the case bits of primary and secondary elements
					tertiary ^= CASE_MASK;
					if (tertiary < (TER_UPPER_FIRST_COMMON_HIGH << 8)) {
						tertiary -= 0x4000;
					}
				} else {
					// keep the uppercase bits of tertiary elements
					tertiary += 0x4000;
				}
				if (common_tertiaries != 0) {
					common_tertiaries--;
					while (common_tertiaries >= TER_UPPER_FIRST_COMMON_MAX_COUNT) {
						tertiaries.AppendByte(TER_UPPER_FIRST_COMMON_MIDDLE);
						common_tertiaries -= TER_UPPER_FIRST_COMMON_MAX_COUNT;
					}
					auto byte = tertiary < (TER_UPPER_FIRST_COMMON_LOW << 8)
					                ? TER_UPPER_FIRST_COMMON_LOW + static_cast<uint32_t>(common_tertiaries)
					                : TER_UPPER_FIRST_COMMON_HIGH - static_cast<uint32_t>(common_tertiaries);
					tertiaries.AppendByte(byte);
					common_tertiaries = 0;
				}
				tertiaries.AppendWeight16(tertiary);
			}
		}

		if ((levels & QUATERNARY_LEVEL_FLAG) != 0) {
			auto quaternary = lower & 0xFFFF;
			if ((quaternary & 0xC0) == 0 && quaternary > NO_CE_WEIGHT) {
				common_quaternaries++;
			} else if (quaternary == NO_CE_WEIGHT && !settings.alternate_shifted && quaternaries.IsEmpty()) {
				// with alternate=non-ignorable and only common weights nothing needs to be written
				quaternaries.AppendByte(LEVEL_SEPARATOR_BYTE);
			} else {
				if (quaternary == NO_CE_WEIGHT) {
					quaternary = LEVEL_SEPARATOR_BYTE;
				} else {
					quaternary = 0xFC + ((quaternary >> 6) & 3);
				}
				if (common_quaternaries != 0) {
					common_quaternaries--;
					while (common_quaternaries >= QUAT_COMMON_MAX_COUNT) {
						quaternaries.AppendByte(QUAT_COMMON_MIDDLE);
						common_quaternaries -= QUAT_COMMON_MAX_COUNT;
					}
					auto byte = quaternary < QUAT_COMMON_LOW
					                ? QUAT_COMMON_LOW + static_cast<uint32_t>(common_quaternaries)
					                : QUAT_COMMON_HIGH - static_cast<uint32_t>(common_quaternaries);
					quaternaries.AppendByte(byte);
					common_quaternaries = 0;
				}
				quaternaries.AppendByte(quaternary);
			}
		}

		if ((lower >> 24) == LEVEL_SEPARATOR_BYTE) {
			// the sentinel element that terminates the levels
			break;
		}
	}

	if ((levels & SECONDARY_LEVEL_FLAG) != 0) {
		result.push_back(LEVEL_SEPARATOR_BYTE);
		secondaries.AppendTo(result);
	}
	if ((levels & CASE_LEVEL_FLAG) != 0) {
		result.push_back(LEVEL_SEPARATOR_BYTE);
		// the nibbles are written as pairs, except for separator bytes
		uint8_t pending = 0;
		for (idx_t i = 0; i + 1 < cases.Size(); i++) {
			auto value = cases[i];
			if (pending == 0) {
				pending = value;
			} else {
				result.push_back(static_cast<uint8_t>(pending | (value >> 4)));
				pending = 0;
			}
		}
		if (pending != 0) {
			result.push_back(pending);
		}
	}
	if ((levels & TERTIARY_LEVEL_FLAG) != 0) {
		result.push_back(LEVEL_SEPARATOR_BYTE);
		tertiaries.AppendTo(result);
	}
	if ((levels & QUATERNARY_LEVEL_FLAG) != 0) {
		result.push_back(LEVEL_SEPARATOR_BYTE);
		quaternaries.AppendTo(result);
	}
}

//===--------------------------------------------------------------------===//
// Collator
//===--------------------------------------------------------------------===//

static const CollationTailoring *FindTailoring(const string &collation) {
	uint32_t lower = 0;
	uint32_t upper = tailoring_count;
	while (lower < upper) {
		auto middle = (lower + upper) / 2;
		auto comparison = collation.compare(tailorings[middle].name);
		if (comparison < 0) {
			upper = middle;
		} else if (comparison > 0) {
			lower = middle + 1;
		} else {
			return tailorings + middle;
		}
	}
	return nullptr;
}

Collator::Collator(const string &collation) : tailoring(FindTailoring(StringUtil::Lower(collation))) {
	if (!tailoring) {
		return;
	}
	settings.strength = static_cast<CollationStrength>(tailoring->strength);
	settings.case_level = tailoring->case_level;
	settings.case_first = static_cast<CaseFirst>(tailoring->case_first);
	settings.alternate_shifted = tailoring->alternate_shifted;
	settings.backward_secondary = tailoring->backward_secondary;
	settings.normalization = tailoring->normalization;
}

bool Collator::HasCollation(const string &collation) {
	return FindTailoring(StringUtil::Lower(collation)) != nullptr;
}

vector<string> Collator::GetCollations() {
	vector<string> result;
	for (uint32_t i = 0; i < tailoring_count; i++) {
		result.emplace_back(tailorings[i].name);
	}
	return result;
}

void Collator::GetSortKey(const char *data, idx_t size, CollationBuffer &buffer) const {
	auto &text = buffer.text;
	Normalizer::Decode(data, size, text);
	if (settings.normalization && !Normalizer::IsFCD(text)) {
		Normalizer::Decompose(text);
	} else {
		Normalizer::DecomposeHangul(text);
	}
	buffer.elements.clear();
	ElementGenerator generator(tailoring, text);
	generator.Generate(buffer.elements);

	buffer.key.clear();
	WriteSortKey(buffer.elements, settings, tailoring ? tailoring->reorder_table : nullptr, buffer);
	buffer.key.push_back(0);
}

} // namespace collation
} // namespace duckdb
