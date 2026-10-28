#include "catch.hpp"
#include "duckdb/common/swar.hpp"

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
	// flag counting and the first flagged byte on every subset of lanes
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
		if (subset) {
			REQUIRE(SwarWord::FirstFlagged(mask) == expected_first);
		}
	}
	uint8_t bytes[SwarWord::SIZE] = {1, 2, 3, 4, 5, 6, 7, 8};
	REQUIRE(SwarWord::SumBytes(WordOf(bytes)) == 36);
	REQUIRE(SwarWord::SumBytes(SwarWord::Repeat(31)) == 248);
	REQUIRE(SwarWord::Repeat(0x2c) == 0x2c2c2c2c2c2c2c2cULL);
}
