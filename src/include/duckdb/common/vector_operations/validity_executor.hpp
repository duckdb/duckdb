//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/vector_operations/validity_executor.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/bit_utils.hpp"
#include "duckdb/common/types/validity_mask.hpp"

namespace duckdb {

// These helpers sit in the inner validity-dispatch loop. Forcing inlining significantly
// improves overall performance.
#if defined(__GNUC__) || defined(__clang__)
#define DUCKDB_VALIDITY_EXECUTOR_ALWAYS_INLINE inline __attribute__((always_inline))
#elif defined(_MSC_VER)
#define DUCKDB_VALIDITY_EXECUTOR_ALWAYS_INLINE __forceinline
#else
#define DUCKDB_VALIDITY_EXECUTOR_ALWAYS_INLINE inline
#endif

//! A normalized slice of one ValidityMask word.
struct ValidityWordSlice {
public:
	explicit ValidityWordSlice(validity_t bits_p, idx_t count_p)
	    : bits(NormalizeBits(bits_p, count_p)), count(count_p) {
	}

	inline validity_t Bits() const {
		return bits;
	}

	inline idx_t Count() const {
		return count;
	}

	inline bool RowIsValid(idx_t offset) const {
		D_ASSERT(offset < count);
		return ValidityMask::RowIsValid(bits, offset);
	}

	inline bool AllValid() const {
		D_ASSERT(count > 0);
		return bits == ValidityMask::EntryWithValidBits(count);
	}

	inline bool HasValid() const {
		D_ASSERT(count > 0);
		return bits != 0;
	}

	inline bool NoneValid() const {
		D_ASSERT(count > 0);
		return bits == 0;
	}

	inline bool HasInvalid() const {
		D_ASSERT(count > 0);
		return bits != ValidityMask::EntryWithValidBits(count);
	}

	//! Counts consecutive runs starting at `offset` and stopping at this slice's count.
	inline idx_t RunLength(idx_t offset) const {
		D_ASSERT(count > 0);
		D_ASSERT(offset < count);

		const auto is_valid = RowIsValid(offset);
		const auto run_bits = is_valid ? bits : ~bits;
		return GetRunLength(run_bits, offset);
	}

	inline ValidityWordSlice Slice(idx_t offset, idx_t slice_count) const {
		D_ASSERT(slice_count > 0);
		D_ASSERT(offset + slice_count <= count);
		return ValidityWordSlice(bits >> offset, slice_count);
	}

private:
	static inline validity_t NormalizeBits(validity_t bits, idx_t count) {
		D_ASSERT(count <= ValidityMask::BITS_PER_VALUE);
		return bits & ValidityMask::EntryWithValidBits(count);
	}

	//! Counts consecutive set bits in the `run_bits` ValidityMast, starting at `offset` and stopping at this slice's
	//! count.
	inline idx_t GetRunLength(validity_t run_bits, idx_t offset) const {
		D_ASSERT(offset < count);

		const auto bits_left = count - offset;
		const auto active_mask = ValidityMask::EntryWithValidBits(bits_left);
		// Move offset to bit 0, then keep only the rows that remain in the slice.
		const auto shifted_bits = (run_bits >> offset) & active_mask;
		const auto stop_bits = (~shifted_bits) & active_mask;
		if (!stop_bits) {
			return bits_left;
		}
		return CountZeros<validity_t>::Trailing(stop_bits);
	}

private:
	const validity_t bits;
	const idx_t count;
};

struct ValidityExecutor {
private:
	template <idx_t RUN_LENGTH>
	static DUCKDB_VALIDITY_EXECUTOR_ALWAYS_INLINE validity_t SetBitRunStartMask(validity_t bits) {
		// Returns a mask of possible run starts. Bit i is set in the result only when the RUN_LENGTH input bits
		// starting at i are all set. For example, a 5-row run with RUN_LENGTH = 3 produces three start candidates:
		//
		// bit position:  4 3 2 1 0
		// bits:          1 1 1 1 1
		// candidate 1:       - - ^
		// candidate 2:     - - ^
		// candidate 3:   - - ^
		//
		// As a worked example, SetBitRunStartMask<3>(bits) is called with:
		//
		// bit position:             9 8 7 6 5 4 3 2 1 0
		// bits:                     1 1 0 1 1 1 0 1 1 1
		//
		// Please read the odd branch, then follow the recursive call stack.
		//
		// returns:                  0 0 0 0 0 1 0 0 0 1
		//                                 - - ^   - - ^
		//                                     |       +-- since original bits 0, 1, 2 are set
		//                                     +---------- since original bits 4, 5, 6 are set
		//
		static_assert(RUN_LENGTH > 0, "RUN_LENGTH must be positive");

		if constexpr (RUN_LENGTH > ValidityMask::BITS_PER_VALUE) {
			return 0;
		} else if constexpr (RUN_LENGTH == 1) {
			// Handles one bit runs, which is just an identity function.
			// Eg for SetBitRunStartMask<1>(bits)
			//
			// bits:                     1 1 0 1 1 1 0 1 1 1
			//
			// return bits

			return bits;
		} else if constexpr ((RUN_LENGTH & (RUN_LENGTH - 1)) == 0) {
			// Handles even numbered bit runs.
			// Eg for SetBitRunStartMask<2>(bits)
			//
			// half_runs = b:            0 1 1 0 1 1 1 0 1 1
			// c = half_runs >> 1:       0 0 1 1 0 1 1 1 0 1
			// d = half_runs & c:        0 0 1 0 0 1 1 0 0 1
			//
			// return d

			const auto half_runs = SetBitRunStartMask<RUN_LENGTH / 2>(bits);
			return half_runs & (half_runs >> (RUN_LENGTH / 2));
		} else {
			// Handles odd numbered bit runs.
			// Eg for SetBitRunStartMask<3>(bits)
			//
			// LEFT_RUN_LENGTH  = 3 / 2: 1
			// RIGHT_RUN_LENGTH = 3 - 1: 2
			//
			// a = bits:                1 1 0 1 1 1 0 1 1 1
			// b = bits >> 1:           0 1 1 0 1 1 1 0 1 1
			//
			// left: SetBitRunStartMask<1>(a)
			// right: SetBitRunStartMask<2>(b)
			//
			// left:                    1 1 0 1 1 1 0 1 1 1
			// right:                   0 0 1 0 0 1 1 0 0 1
			//
			// return left & right:     0 0 0 0 0 1 0 0 0 1

			constexpr idx_t LEFT_RUN_LENGTH = RUN_LENGTH / 2;
			constexpr idx_t RIGHT_RUN_LENGTH = RUN_LENGTH - LEFT_RUN_LENGTH;
			return SetBitRunStartMask<LEFT_RUN_LENGTH>(bits) &
			       SetBitRunStartMask<RIGHT_RUN_LENGTH>(bits >> LEFT_RUN_LENGTH);
		}
	}

	static DUCKDB_VALIDITY_EXECUTOR_ALWAYS_INLINE validity_t SetBitRunFirstBitMask(validity_t bits) {
		// SetBitRunFirstBitMask(bits) returns a mask with the first 1 bit of each consecutive 1 run.
		// Eg:
		// bit position:      9 8 7 6 5 4 3 2 1 0
		// bits:              0 1 1 1 0 0 1 1 0 1
		// a =   bits << 1:   1 1 1 0 0 1 1 0 1 0
		// b = ~(bits << 1):  0 0 0 1 1 0 0 1 0 1
		// return a & b:      0 0 0 1 0 0 0 1 0 1

		return bits & ~(bits << 1);
	}

	static DUCKDB_VALIDITY_EXECUTOR_ALWAYS_INLINE validity_t SetBitRunLastBitMask(validity_t bits) {
		// SetBitRunLastBitMask(bits) returns a mask with the last 1 bit of each consecutive 1 run.
		// Eg:
		// bit position:      9 8 7 6 5 4 3 2 1 0
		// bits:              0 1 1 1 0 0 1 1 0 1
		//       bits >> 1:   0 0 1 1 1 0 0 1 1 0
		// b = ~(bits >> 1):  1 1 0 0 0 1 1 0 0 1
		// return bits & b:   0 1 0 0 0 0 1 0 0 1

		return bits & ~(bits >> 1);
	}

	static DUCKDB_VALIDITY_EXECUTOR_ALWAYS_INLINE validity_t InvertedBitMask(validity_t bits, validity_t active_mask) {
		// InvertedBitMask(bits, active_mask) returns the inverse of bits within active_mask.
		// Eg:
		// bit position:     9 8 7 6 5 4 3 2 1 0
		// bits:             1 0 1 1 0 0 1 0 1 1
		// a = ~bits:        0 1 0 0 1 1 0 1 0 0
		// active_mask:      0 0 0 0 1 1 1 1 1 1
		// return a & mask:  0 0 0 0 1 1 0 1 0 0

		return (~bits) & active_mask;
	}

	static DUCKDB_VALIDITY_EXECUTOR_ALWAYS_INLINE validity_t SingleBitMask(idx_t offset) {
		// SingleBitMask(offset) returns a mask with bit offset set.
		// Eg, offset = 4:
		// bit position:        9 8 7 6 5 4 3 2 1 0
		// validity_t(1):       0 0 0 0 0 0 0 0 0 1
		// return 1 << offset:  0 0 0 0 0 1 0 0 0 0

		D_ASSERT(offset < ValidityMask::BITS_PER_VALUE);
		return validity_t(1) << offset;
	}

	static DUCKDB_VALIDITY_EXECUTOR_ALWAYS_INLINE bool MaskContainsBit(validity_t bits, idx_t offset) {
		// MaskContainsBit(bits, offset) returns true when bit offset is set in bits.
		// Eg, offset = 4:
		// bit position:           9 8 7 6 5 4 3 2 1 0
		// bits:                   1 0 1 1 0 1 0 0 1 0
		// SingleBitMask(offset):  0 0 0 0 0 1 0 0 0 0
		// bits & mask:            0 0 0 0 0 1 0 0 0 0
		// return:                 true

		// Eg, offset = 5:
		// bit position:           9 8 7 6 5 4 3 2 1 0
		// bits:                   1 0 1 1 0 1 0 0 1 0
		// SingleBitMask(offset):  0 0 0 0 1 0 0 0 0 0
		// bits & mask:            0 0 0 0 0 0 0 0 0 0
		// return:                 false

		return (bits & SingleBitMask(offset)) != 0;
	}

	static DUCKDB_VALIDITY_EXECUTOR_ALWAYS_INLINE validity_t BitsAtOrAfter(idx_t offset) {
		// BitsAtOrAfter(offset) returns a mask with bit offset and every higher bit set.
		// Eg, offset = 4:
		// bit position:    9 8 7 6 5 4 3 2 1 0
		// ~validity_t(0):  1 1 1 1 1 1 1 1 1 1
		// return:          1 1 1 1 1 1 0 0 0 0

		D_ASSERT(offset < ValidityMask::BITS_PER_VALUE);
		return ~validity_t(0) << offset;
	}

	static DUCKDB_VALIDITY_EXECUTOR_ALWAYS_INLINE idx_t FirstSetBit(validity_t bits) {
		// FirstSetBit(bits) returns the lowest set bit position.
		// Eg:
		// bit position:         9 8 7 6 5 4 3 2 1 0
		// bits:                 0 1 1 0 1 0 0 0 0 0
		// trailing zero count:            4 3 2 1 0
		// return:                         4

		D_ASSERT(bits);
		return CountZeros<validity_t>::Trailing(bits);
	}

	static DUCKDB_VALIDITY_EXECUTOR_ALWAYS_INLINE validity_t ClearFirstSetBit(validity_t bits) {
		// ClearFirstSetBit(bits) returns bits with the lowest set bit cleared.
		// Eg:
		// bit position:     9 8 7 6 5 4 3 2 1 0
		// bits:             0 1 1 0 1 0 0 0 0 0
		// a = bits - 1:     0 1 1 0 0 1 1 1 1 1
		// return a & bits:  0 1 1 0 0 0 0 0 0 0

		D_ASSERT(bits);
		return bits & (bits - 1);
	}

	template <idx_t MIN_EXTRACT_RUN_LENGTH, class VALID_FUNC, class VALIDITY_SLICE_FUNC>
	static DUCKDB_VALIDITY_EXECUTOR_ALWAYS_INLINE void ProcessAllValidNoMask(idx_t count, VALID_FUNC &valid_func,
	                                                                         VALIDITY_SLICE_FUNC &validity_slice_func) {
		if (count >= MIN_EXTRACT_RUN_LENGTH) {
			valid_func(0, count);
			return;
		}

		idx_t local_offset = 0;
		const auto full_word_count = count / ValidityMask::BITS_PER_VALUE;
		for (idx_t word_idx = 0; word_idx < full_word_count; word_idx++) {
			validity_slice_func(local_offset,
			                    ValidityWordSlice(ValidityMask::EntryWithValidBits(ValidityMask::BITS_PER_VALUE),
			                                      ValidityMask::BITS_PER_VALUE));
			local_offset += ValidityMask::BITS_PER_VALUE;
		}

		if (local_offset < count) {
			const auto slice_count = count - local_offset;
			validity_slice_func(local_offset,
			                    ValidityWordSlice(ValidityMask::EntryWithValidBits(slice_count), slice_count));
		}
	}

	template <idx_t MIN_EXTRACT_RUN_LENGTH, class VALID_FUNC, class INVALID_FUNC, class VALIDITY_SLICE_FUNC>
	static DUCKDB_VALIDITY_EXECUTOR_ALWAYS_INLINE void
	ProcessFullValidityWordSlice(idx_t local_offset, validity_t entry, VALID_FUNC &valid_func,
	                             INVALID_FUNC &invalid_func, VALIDITY_SLICE_FUNC &validity_slice_func) {
		if (ValidityMask::AllValid(entry)) {
			if constexpr (ValidityMask::BITS_PER_VALUE >= MIN_EXTRACT_RUN_LENGTH) {
				valid_func(local_offset, ValidityMask::BITS_PER_VALUE);
			} else {
				validity_slice_func(local_offset, ValidityWordSlice(entry, ValidityMask::BITS_PER_VALUE));
			}
			return;
		}

		if (ValidityMask::NoneValid(entry)) {
			if constexpr (ValidityMask::BITS_PER_VALUE >= MIN_EXTRACT_RUN_LENGTH) {
				invalid_func(local_offset, ValidityMask::BITS_PER_VALUE);
			} else {
				validity_slice_func(local_offset, ValidityWordSlice(entry, ValidityMask::BITS_PER_VALUE));
			}
			return;
		}

		ProcessMixedValidityWordSlice<MIN_EXTRACT_RUN_LENGTH>(local_offset,
		                                                      ValidityWordSlice(entry, ValidityMask::BITS_PER_VALUE),
		                                                      valid_func, invalid_func, validity_slice_func);
	}

	template <idx_t MIN_EXTRACT_RUN_LENGTH, class VALID_FUNC, class INVALID_FUNC, class VALIDITY_SLICE_FUNC>
	static DUCKDB_VALIDITY_EXECUTOR_ALWAYS_INLINE void
	ProcessPartialValidityWordSlice(idx_t local_offset, validity_t entry, idx_t idx_in_entry, idx_t slice_count,
	                                VALID_FUNC &valid_func, INVALID_FUNC &invalid_func,
	                                VALIDITY_SLICE_FUNC &validity_slice_func) {
		D_ASSERT(idx_in_entry < ValidityMask::BITS_PER_VALUE);
		D_ASSERT(slice_count > 0);
		D_ASSERT(slice_count <= ValidityMask::BITS_PER_VALUE - idx_in_entry);

		// The raw word still uses the ValidityMask's word-local bit positions. The requested slice starts at
		// idx_in_entry, so shift that down to local row 0, then keep only slice_count rows.
		// Eg,
		//
		// idx_in_entry = 3 and slice_count = 4 maps ValidityMask word bits 3..6 to local bits 0..3.
		//
		// bit position:                    9 8 7 6 5 4 3 2 1 0
		// entry:                           1 0 1 1 0 1 1 0 0 1
		// a = shifted_entry = entry >> 3:  0 0 0 1 0 1 1 0 1 1
		// b = active_mask:                 0 0 0 0 0 0 1 1 1 1
		// valid_bits = a & b =             0 0 0 0 0 0 1 0 1 1
		const auto shifted_entry = entry >> idx_in_entry;
		const auto active_mask = ValidityMask::EntryWithValidBits(slice_count);
		const auto valid_bits = shifted_entry & active_mask;
		ProcessValidityWordSlice<MIN_EXTRACT_RUN_LENGTH>(local_offset, ValidityWordSlice(valid_bits, slice_count),
		                                                 valid_func, invalid_func, validity_slice_func);
	}

	template <idx_t MIN_EXTRACT_RUN_LENGTH, class VALID_FUNC, class INVALID_FUNC, class VALIDITY_SLICE_FUNC>
	static DUCKDB_VALIDITY_EXECUTOR_ALWAYS_INLINE void
	ProcessValidityWordSlice(idx_t local_offset, const ValidityWordSlice &word, VALID_FUNC &valid_func,
	                         INVALID_FUNC &invalid_func, VALIDITY_SLICE_FUNC &validity_slice_func) {
		D_ASSERT(word.Count() > 0);
		if (word.AllValid()) {
			if (word.Count() >= MIN_EXTRACT_RUN_LENGTH) {
				valid_func(local_offset, word.Count());
			} else {
				validity_slice_func(local_offset, word);
			}
			return;
		}

		if (word.NoneValid()) {
			if (word.Count() >= MIN_EXTRACT_RUN_LENGTH) {
				invalid_func(local_offset, word.Count());
			} else {
				validity_slice_func(local_offset, word);
			}
			return;
		}

		ProcessMixedValidityWordSlice<MIN_EXTRACT_RUN_LENGTH>(local_offset, word, valid_func, invalid_func,
		                                                      validity_slice_func);
	}

	template <idx_t MIN_EXTRACT_RUN_LENGTH, class VALID_FUNC, class INVALID_FUNC, class VALIDITY_SLICE_FUNC>
	static DUCKDB_VALIDITY_EXECUTOR_ALWAYS_INLINE void
	ProcessMixedValidityWordSlice(idx_t local_offset, const ValidityWordSlice &word, VALID_FUNC &valid_func,
	                              INVALID_FUNC &invalid_func, VALIDITY_SLICE_FUNC &validity_slice_func) {
		D_ASSERT(word.Count() > 0);
		D_ASSERT(!word.AllValid());
		D_ASSERT(!word.NoneValid());
		// Every mask below uses slice-local row offsets: bit i describes row i in this slice.

		if constexpr (MIN_EXTRACT_RUN_LENGTH >= ValidityMask::BITS_PER_VALUE) {
			// If the min extract length is longer than the word size, this whole function should boil off,
			// calling validity_slice_func instead.
			validity_slice_func(local_offset, word);
			return;
		} else {
			if (word.Count() < MIN_EXTRACT_RUN_LENGTH) {
				// Fewer bits set than the minimum run length guarantees a lack of runs
				validity_slice_func(local_offset, word);
				return;
			}

			const auto active_mask = ValidityMask::EntryWithValidBits(word.Count());
			const auto valid_bits = word.Bits();
			const auto invalid_bits = InvertedBitMask(valid_bits, active_mask);

			const auto valid_run_candidates = SetBitRunStartMask<MIN_EXTRACT_RUN_LENGTH>(valid_bits);
			const auto invalid_run_candidates = SetBitRunStartMask<MIN_EXTRACT_RUN_LENGTH>(invalid_bits);
			const auto run_candidates = valid_run_candidates | invalid_run_candidates;
			if (!run_candidates) {
				// No runs, send to the mixed callback.
				validity_slice_func(local_offset, word);
				return;
			}

			const auto valid_run_starts = valid_run_candidates & SetBitRunFirstBitMask(valid_bits);
			const auto invalid_run_starts = invalid_run_candidates & SetBitRunFirstBitMask(invalid_bits);
			auto run_starts = valid_run_starts | invalid_run_starts;
			// Candidate masks mark every position where MIN_EXTRACT_RUN_LENGTH rows fit, including positions inside a
			// longer run. Intersecting with the first-bit masks leaves exactly one start for each extractable run.
			const auto valid_run_ends = SetBitRunLastBitMask(valid_bits);
			const auto invalid_run_ends = SetBitRunLastBitMask(invalid_bits);

			idx_t slice_start = 0;
			while (run_starts) {
				const auto run_offset = FirstSetBit(run_starts);
				D_ASSERT(run_offset >= slice_start);

				const auto is_valid = MaskContainsBit(valid_run_starts, run_offset);
				const auto run_end_mask = is_valid ? valid_run_ends : invalid_run_ends;
				// Once a run is worth extracting, emit the entire run.
				// Ends before run_offset belong to earlier runs of the same state, so ignore them.
				const auto run_end_candidates = run_end_mask & BitsAtOrAfter(run_offset);
				D_ASSERT(run_end_candidates);
				const auto run_end = FirstSetBit(run_end_candidates);
				const auto run_count = run_end - run_offset + 1;
				D_ASSERT(run_count >= MIN_EXTRACT_RUN_LENGTH);

				if (slice_start < run_offset) {
					validity_slice_func(local_offset + slice_start, word.Slice(slice_start, run_offset - slice_start));
				}
				if (is_valid) {
					valid_func(local_offset + run_offset, run_count);
				} else {
					invalid_func(local_offset + run_offset, run_count);
				}

				slice_start = run_offset + run_count;
				run_starts = ClearFirstSetBit(run_starts);
			}

			if (slice_start < word.Count()) {
				validity_slice_func(local_offset + slice_start, word.Slice(slice_start, word.Count() - slice_start));
			}
		}
	}

public:
	//! Walk a slice of a ValidityMask and dispatch local validity work.
	//! Callback offsets are local to [source_offset, source_offset + count): callback offset 0 is source_offset in the
	//! input mask. valid_func and invalid_func receive uniform runs of at least MIN_EXTRACT_RUN_LENGTH rows.
	//! validity_slice_func receives the remaining pieces that still need bitwise validity checks.
	template <idx_t MIN_EXTRACT_RUN_LENGTH, class VALID_FUNC, class INVALID_FUNC, class VALIDITY_SLICE_FUNC>
	static DUCKDB_VALIDITY_EXECUTOR_ALWAYS_INLINE void
	Execute(const ValidityMask &validity, idx_t source_offset, idx_t count, VALID_FUNC &&valid_func,
	        INVALID_FUNC &&invalid_func, VALIDITY_SLICE_FUNC &&validity_slice_func) {
		static_assert(MIN_EXTRACT_RUN_LENGTH > 0, "MIN_EXTRACT_RUN_LENGTH must be positive");

		if (count == 0) {
			return;
		}

		if (validity.CannotHaveNull()) {
			ProcessAllValidNoMask<MIN_EXTRACT_RUN_LENGTH>(count, valid_func, validity_slice_func);
			return;
		}

		idx_t entry_idx;
		idx_t idx_in_entry;
		ValidityMask::GetEntryIndex(source_offset, entry_idx, idx_in_entry);

		idx_t local_offset = 0;
		// If source_offset starts in the middle of a ValidityMask word, normalize that prefix before processing whole
		// words.
		if (DUCKDB_UNLIKELY(idx_in_entry != 0)) {
			const auto slice_count = MinValue<idx_t>(ValidityMask::BITS_PER_VALUE - idx_in_entry, count);
			const auto entry = validity.GetValidityEntryUnsafe(entry_idx++);
			ProcessPartialValidityWordSlice<MIN_EXTRACT_RUN_LENGTH>(local_offset, entry, idx_in_entry, slice_count,
			                                                        valid_func, invalid_func, validity_slice_func);
			local_offset += slice_count;
		}

		// Process ValidityMask::BITS_PER_VALUE sized chunks
		const auto full_word_count = (count - local_offset) / ValidityMask::BITS_PER_VALUE;
		for (idx_t word_idx = 0; word_idx < full_word_count; word_idx++) {
			const auto entry = validity.GetValidityEntryUnsafe(entry_idx++);
			ProcessFullValidityWordSlice<MIN_EXTRACT_RUN_LENGTH>(local_offset, entry, valid_func, invalid_func,
			                                                     validity_slice_func);
			local_offset += ValidityMask::BITS_PER_VALUE;
		}

		// Process the tail.
		if (local_offset < count) {
			const auto entry = validity.GetValidityEntryUnsafe(entry_idx);
			ProcessPartialValidityWordSlice<MIN_EXTRACT_RUN_LENGTH>(local_offset, entry, 0, count - local_offset,
			                                                        valid_func, invalid_func, validity_slice_func);
		}
	}
};

#undef DUCKDB_VALIDITY_EXECUTOR_ALWAYS_INLINE

} // namespace duckdb
