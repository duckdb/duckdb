#include "catch.hpp"
#include "duckdb/common/bit_utils.hpp"
#include "duckdb/common/swar.hpp"
#include "duckdb/common/vector.hpp"

#include <random>

using namespace duckdb;

namespace {

uint64_t WordOf(const uint8_t *bytes) {
	return Load<uint64_t>(bytes);
}

uint64_t ReferenceZeroBytes(const uint8_t *bytes) {
	// flags in memory order, as a word
	uint8_t flags[SwarWord::SIZE];
	for (idx_t i = 0; i < SwarWord::SIZE; i++) {
		flags[i] = bytes[i] == 0 ? 0x80 : 0;
	}
	return WordOf(flags);
}

//! A byte pattern as value and mask, before the repetition over a word
struct Pattern {
	uint8_t value;
	uint8_t mask;
};

//! The bytes of the block that match a pattern exactly, what MaybeAnyMask may only add flags to
uint64_t ReferenceAnyMask(const char *block, const duckdb::vector<Pattern> &patterns) {
	uint64_t mask = 0;
	for (idx_t i = 0; i < SwarBlock::SIZE; i++) {
		for (auto &pattern : patterns) {
			if ((static_cast<uint8_t>(block[i]) & pattern.mask) == (pattern.value & pattern.mask)) {
				mask |= uint64_t(1) << i;
			}
		}
	}
	return mask;
}

duckdb::vector<SwarBlock::BytePattern> Repeated(const duckdb::vector<Pattern> &patterns) {
	duckdb::vector<SwarBlock::BytePattern> repeated;
	for (auto &pattern : patterns) {
		repeated.emplace_back(pattern.value, pattern.mask);
	}
	return repeated;
}

uint64_t MaybeAnyMaskOf(const char *block, const duckdb::vector<Pattern> &patterns) {
	return SwarBlock::MaybeAnyMask(const_data_ptr_cast(block), Repeated(patterns));
}

//! Every match is flagged, and within a word the first flag is always a match, so false flags only follow one
void RequireSuperset(const uint64_t exact, const uint64_t maybe) {
	REQUIRE((exact & ~maybe) == 0);
	for (idx_t word = 0; word < SwarBlock::WORDS; word++) {
		const uint64_t word_mask = uint64_t(0xff) << (word * SwarWord::SIZE);
		const uint64_t maybe_word = maybe & word_mask;
		if (maybe_word) {
			REQUIRE(CountZeros<uint64_t>::Trailing(maybe_word) == CountZeros<uint64_t>::Trailing(exact & word_mask));
		}
	}
}

} // namespace

TEST_CASE("SWAR word flags", "[swar]") {
	// every pair of byte values, in the first two lanes, with a third distinct lane
	for (idx_t a = 0; a < 256; a++) {
		for (idx_t b = 0; b < 256; b++) {
			uint8_t bytes[SwarWord::SIZE] = {
			    static_cast<uint8_t>(a), static_cast<uint8_t>(b), 0x42, 0x42, 0x42, 0x42, 0x42, 0x42};
			const auto word = WordOf(bytes);
			const auto exact = SwarWord::ZeroBytes(word);
			REQUIRE(exact == ReferenceZeroBytes(bytes));
			REQUIRE(SwarWord::IsAscii(word) == (a < 0x80 && b < 0x80));
			// the inexact test flags every zero byte and nothing when there is none
			const auto maybe = SwarWord::MaybeZeroBytes(word);
			REQUIRE((exact & ~maybe) == 0);
			if (!exact) {
				REQUIRE(maybe == 0);
			}
		}
	}
	// flag counting, first flagged byte and packing on every subset of lanes
	for (idx_t subset = 0; subset < 256; subset++) {
		uint8_t bytes[SwarWord::SIZE];
		idx_t expected_count = 0;
		idx_t expected_first = SwarWord::SIZE;
		for (idx_t i = 0; i < SwarWord::SIZE; i++) {
			const bool flagged = (subset >> i) & 1;
			bytes[i] = flagged ? 0 : 1;
			expected_count += flagged;
			if (flagged && expected_first == SwarWord::SIZE) {
				expected_first = i;
			}
		}
		const auto mask = SwarWord::ZeroBytes(WordOf(bytes));
		REQUIRE(SwarWord::CountFlagged(mask) == expected_count);
		REQUIRE(SwarWord::PackFlags(mask) == subset);
		if (subset) {
			REQUIRE(SwarWord::FirstFlagged(mask) == expected_first);
		}
	}
	uint8_t bytes[SwarWord::SIZE] = {1, 2, 3, 4, 5, 6, 7, 8};
	REQUIRE(SwarWord::SumBytes(WordOf(bytes)) == 36);
	REQUIRE(SwarWord::SumBytes(SwarWord::Repeat(31)) == 248);
}

TEST_CASE("SWAR block mask over every byte value", "[swar]") {
	// four blocks that together hold every byte value exactly once
	char blocks[4][SwarBlock::SIZE];
	for (idx_t b = 0; b < 4; b++) {
		for (idx_t i = 0; i < SwarBlock::SIZE; i++) {
			blocks[b][i] = static_cast<char>(b * SwarBlock::SIZE + i);
		}
	}
	for (idx_t c = 0; c < 256; c++) {
		const duckdb::vector<Pattern> pattern = {{static_cast<uint8_t>(c), 0xff}};
		idx_t hits = 0;
		for (idx_t b = 0; b < 4; b++) {
			const auto exact = ReferenceAnyMask(blocks[b], pattern);
			const auto maybe = MaybeAnyMaskOf(blocks[b], pattern);
			RequireSuperset(exact, maybe);
			if (maybe) {
				hits++;
				REQUIRE(b == c / SwarBlock::SIZE);
				REQUIRE(exact == uint64_t(1) << (c % SwarBlock::SIZE));
			}
		}
		REQUIRE(hits == 1);
	}
}

TEST_CASE("SWAR block mask on random blocks", "[swar]") {
	std::mt19937 gen(42);
	// alphabets, CSV structure, the byte values that trip inexact zero byte tests, and everything
	const duckdb::vector<duckdb::vector<uint8_t>> alphabets = {
	    {',', '\n', '"', 'a'},
	    {0x00, 0x01, 0x7f, 0x80, 0xff, ','},
	};
	for (idx_t alphabet_idx = 0; alphabet_idx <= alphabets.size(); alphabet_idx++) {
		const bool full_range = alphabet_idx == alphabets.size();
		for (idx_t round = 0; round < 1000; round++) {
			char block[SwarBlock::SIZE];
			for (idx_t i = 0; i < SwarBlock::SIZE; i++) {
				if (full_range) {
					block[i] = static_cast<char>(gen() & 0xff);
				} else {
					auto &alphabet = alphabets[alphabet_idx];
					block[i] = static_cast<char>(alphabet[gen() % alphabet.size()]);
				}
			}
			for (idx_t c = 0; c < 256; c++) {
				const duckdb::vector<Pattern> pattern = {{static_cast<uint8_t>(c), 0xff}};
				RequireSuperset(ReferenceAnyMask(block, pattern), MaybeAnyMaskOf(block, pattern));
			}
		}
	}
}

TEST_CASE("SWAR masked patterns", "[swar]") {
	// the byte class 0x08 to 0x0f, bit pattern 00001xxx
	char block[SwarBlock::SIZE];
	for (idx_t i = 0; i < SwarBlock::SIZE; i++) {
		block[i] = static_cast<char>(i * 4 + 1);
	}
	block[3] = '\n';
	block[17] = '\r';
	block[40] = '\t';
	block[63] = static_cast<char>(0x88);
	const duckdb::vector<Pattern> byte_class = {{0x08, 0xf8}};
	const auto exact = ReferenceAnyMask(block, byte_class);
	REQUIRE(exact == ((uint64_t(1) << 2) | (uint64_t(1) << 3) | (uint64_t(1) << 17) | (uint64_t(1) << 40)));
	// a masked byte never takes the value that draws a false flag, so the mask is exact
	REQUIRE(MaybeAnyMaskOf(block, byte_class) == exact);
	// the value is normalized to the mask, so 0x0d names the same class as 0x08
	REQUIRE(MaybeAnyMaskOf(block, {{0x0d, 0xf8}}) == MaybeAnyMaskOf(block, byte_class));
	// a pattern count beyond the supported bound is refused, not silently truncated
	duckdb::vector<Pattern> too_many;
	for (idx_t p = 0; p <= SwarBlock::MAX_PATTERNS; p++) {
		too_many.push_back({static_cast<uint8_t>(p), 0xff});
	}
	REQUIRE_THROWS(MaybeAnyMaskOf(block, too_many));
	REQUIRE_THROWS(MaybeAnyMaskOf(block, {}));
}

TEST_CASE("SWAR superset membership mask", "[swar]") {
	const duckdb::vector<Pattern> patterns = {{',', 0xff}, {'"', 0xff}, {0x08, 0xf8}};
	std::mt19937 gen(7);
	const duckdb::vector<uint8_t> alphabet = {',', '"', '\n', '\r', '\t', 'a', 0x00, 0x01, 0x80, 0xff};
	for (idx_t round = 0; round < 2000; round++) {
		char block[SwarBlock::SIZE];
		for (idx_t i = 0; i < SwarBlock::SIZE; i++) {
			block[i] = static_cast<char>(round < 1000 ? alphabet[gen() % alphabet.size()] : gen() & 0xff);
		}
		RequireSuperset(ReferenceAnyMask(block, patterns), MaybeAnyMaskOf(block, patterns));
	}
	// every pattern count up to the bound dispatches
	for (idx_t count = 1; count <= SwarBlock::MAX_PATTERNS; count++) {
		duckdb::vector<Pattern> many;
		for (idx_t p = 0; p < count; p++) {
			many.push_back({static_cast<uint8_t>('a' + p), 0xff});
		}
		char block[SwarBlock::SIZE];
		for (idx_t i = 0; i < SwarBlock::SIZE; i++) {
			block[i] = static_cast<char>('a' + i % 10);
		}
		RequireSuperset(ReferenceAnyMask(block, many), MaybeAnyMaskOf(block, many));
	}
}
