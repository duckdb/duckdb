//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/swar.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/bit_utils.hpp"
#include "duckdb/common/typedefs.hpp"

namespace duckdb {

//! SWAR primitives over the eight bytes of a uint64_t, the masks flag a byte by setting its high bit
struct SwarWord {
	static constexpr idx_t SIZE = sizeof(uint64_t);
	static constexpr uint64_t LSB = 0x0101010101010101ULL;
	static constexpr uint64_t MSB = 0x8080808080808080ULL;
	static constexpr uint64_t LOW7 = 0x7F7F7F7F7F7F7F7FULL;

	//! A word with every byte set to `byte`
	static inline uint64_t Repeat(uint8_t byte) {
		return LSB * byte;
	}

	//! Flags every byte of `word` that is zero, exact for all byte values
	static inline uint64_t ZeroBytes(uint64_t word) {
		return ~(((word & LOW7) + LOW7) | word) & MSB;
	}

	//! Flags every zero byte of `word` and possibly bytes above one, cheaper than ZeroBytes for callers that verify
	static inline uint64_t MaybeZeroBytes(uint64_t word) {
		return (word - LSB) & ~word & MSB;
	}

	//! Whether no byte of `word` has its high bit set
	static inline bool IsAscii(uint64_t word) {
		return (word & MSB) == 0;
	}

	//! Sums up the individual bytes of `word`, only valid if the sum does not exceed 255
	static inline idx_t SumBytes(uint64_t word) {
		return static_cast<idx_t>((word * LSB) >> 56);
	}

	//! The number of flagged bytes in a mask
	static inline idx_t CountFlagged(uint64_t mask) {
		// every flagged byte has its high bit set, shifted down each byte is a zero or a one
		return SumBytes(mask >> 7);
	}

	//! The index (in memory order) of the first flagged byte in a mask
	static inline idx_t FirstFlagged(uint64_t mask) {
#if DUCKDB_IS_BIG_ENDIAN
		return CountZeros<uint64_t>::Leading(mask) / 8;
#else
		return CountZeros<uint64_t>::Trailing(mask) / 8;
#endif
	}
};

} // namespace duckdb
