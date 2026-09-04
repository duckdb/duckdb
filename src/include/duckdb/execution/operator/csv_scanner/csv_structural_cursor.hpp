//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/csv_scanner/csv_structural_cursor.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/bit_utils.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/swar.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/execution/operator/csv_scanner/csv_buffer.hpp"
#include "duckdb/execution/operator/csv_scanner/csv_state_machine_cache.hpp"

namespace duckdb {

//! Finds the structural bytes of a buffer through a stop mask cached for one block of 64 bytes at a time
class CSVStructuralCursor {
public:
	enum class Stop : uint8_t { FOUND, LIMIT, TAIL };

	//! Takes the byte patterns that end a skip, a byte matches a pattern on the bits set in its mask
	explicit CSVStructuralCursor(vector<SwarBlock::BytePattern> patterns_p) : patterns(std::move(patterns_p)) {
		if (patterns.empty() || patterns.size() > SwarBlock::MAX_PATTERNS) {
			throw InternalException("CSVStructuralCursor takes 1 to %d byte patterns", SwarBlock::MAX_PATTERNS);
		}
	}

	//! Binds the cursor to a buffer, dropping the block of a previous one
	void Bind(const CSVBufferHandle &buffer) {
		data = const_data_ptr_cast(buffer.Ptr());
		size = buffer.actual_size;
		start = 0;
		end = 0;
	}

	//! Moves `pos` over unflagged bytes to a flagged one (FOUND), to `limit` (LIMIT) or into the buffer tail (TAIL)
	inline Stop AdvanceToStop(const idx_t limit, idx_t &pos) {
		while (pos < limit) {
			if (pos < start || pos >= end) {
				if (pos + SwarBlock::SIZE > size) {
					return Stop::TAIL;
				}
				LoadBlock(pos);
			}
			const uint64_t remaining = stops >> (pos - start);
			if (remaining) {
				const idx_t stop = pos + CountZeros<uint64_t>::Trailing(remaining);
				if (stop < limit) {
					pos = stop;
					return Stop::FOUND;
				}
			}
			pos = MinValue<idx_t>(remaining ? limit : end, limit);
		}
		return Stop::LIMIT;
	}

	//! Moves `pos` to the next byte `skip_table` does not skip, or to the byte before `to_pos`, through the block
	inline void SkipUntilStop(const bool (&skip_table)[StateMachine::NUM_TRANSITIONS], const idx_t to_pos, idx_t &pos) {
		while (pos + 1 < to_pos) {
			const auto stop = AdvanceToStop(to_pos - 1, pos);
			if (stop == Stop::TAIL) {
				while (skip_table[Byte(pos)] && pos + 1 < to_pos) {
					pos++;
				}
				return;
			}
			if (stop == Stop::LIMIT || !skip_table[Byte(pos)]) {
				return;
			}
			// a flagged byte the skip table skips after all
			pos++;
		}
	}

private:
	uint8_t Byte(const idx_t pos) const {
		return data[pos];
	}

	//! Computes the stop mask of the block that starts at `block_start`
	void LoadBlock(const idx_t block_start) {
		const auto block = data + block_start;
		stops = SwarBlock::MaybeAnyMask(block, patterns);
		start = block_start;
		end = block_start + SwarBlock::SIZE;
	}

	//! The byte patterns that end a skip
	const vector<SwarBlock::BytePattern> patterns;
	//! The bytes of the bound buffer and their count, the block address must not depend on a load chain
	const_data_ptr_t data = nullptr;
	idx_t size = 0;
	//! The loaded block [start, end) and its stop mask, bit i is byte start + i
	idx_t start = 0;
	idx_t end = 0;
	uint64_t stops = 0;
};

} // namespace duckdb
