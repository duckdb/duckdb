//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/compression/roaring/appender.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/types/validity_mask.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/storage/segment/uncompressed.hpp"
#include "duckdb/storage/compression/roaring/roaring.hpp"

namespace duckdb {

namespace roaring {

template <class STATE_TYPE>
struct RoaringStateAppender {
public:
	RoaringStateAppender() = delete;

public:
	static void AppendBytes(STATE_TYPE &state, validity_t entry, idx_t bits) {
		D_ASSERT(bits <= ValidityMask::BITS_PER_VALUE);

		idx_t full_bytes = bits / 8;
		idx_t last_bits = bits % 8;
		for (idx_t i = 0; i < full_bytes; i++) {
			// Create a mask for the byte we care about (least to most significant byte)
			auto bitmask = ValidityUncompressed::UPPER_MASKS[8] >> ((7 - i) * 8);
			// Shift to the least significant bits so we can use it to index our bitmask table
			auto array_index = static_cast<uint8_t>((entry & bitmask) >> (i * 8));
			STATE_TYPE::HandleByte(state, array_index);
		}

		if (DUCKDB_UNLIKELY(last_bits != 0)) {
			auto bitmask = ValidityUncompressed::UPPER_MASKS[8] >> ((7 - full_bytes) * 8);
			auto array_index = static_cast<uint8_t>((entry & bitmask) >> (full_bytes * 8));
			STATE_TYPE::HandleRaggedByte(state, array_index, last_bits);
		}
	}

	static void AppendVector(STATE_TYPE &state, Vector &input, idx_t input_size) {
		UnifiedVectorFormat unified;
		input.ToUnifiedFormat(input_size, unified);
		auto &validity = unified.validity;

		if (validity.AllValid()) {
			// All bits are set implicitly
			idx_t appended = 0;
			while (appended < input_size) {
				idx_t to_append =
				    MinValue<idx_t>(ROARING_CONTAINER_SIZE - STATE_TYPE::Count(state), input_size - appended);
				STATE_TYPE::HandleAllValid(state, to_append);
				if (STATE_TYPE::Count(state) == ROARING_CONTAINER_SIZE) {
					STATE_TYPE::Flush(state);
				}
				appended += to_append;
			}
		} else {
			// There is a validity mask
			idx_t appended = 0;
			while (appended < input_size) {
				idx_t to_append =
				    MinValue<idx_t>(ROARING_CONTAINER_SIZE - STATE_TYPE::Count(state), input_size - appended);

				// Deal with a ragged start, when 'appended' isn't a multiple of ValidityMask::BITS_PER_VALUE
				// then we need to process the remainder of the entry
				idx_t entry_offset = appended / ValidityMask::BITS_PER_VALUE;
				auto previous_entry_remainder = appended % ValidityMask::BITS_PER_VALUE;
				if (DUCKDB_UNLIKELY(previous_entry_remainder != 0)) {
					auto validity_entry = validity.GetValidityEntry(entry_offset);

					idx_t to_process = ValidityMask::BITS_PER_VALUE - previous_entry_remainder;
					validity_t mask;
					if (to_append < to_process) {
						// Limit the amount of bits to set
						auto left_bits = to_process - to_append;
						to_process = to_append;
						mask = ValidityUncompressed::UPPER_MASKS[to_process] >> left_bits;
					} else {
						mask = ValidityUncompressed::UPPER_MASKS[to_process];
					}

					auto masked_entry = (validity_entry & mask) >> previous_entry_remainder;
					if (masked_entry == ValidityUncompressed::LOWER_MASKS[to_process]) {
						// All bits are set
						STATE_TYPE::HandleAllValid(state, to_process);
					} else if (masked_entry == 0) {
						// None of the bits are set
						STATE_TYPE::HandleNoneValid(state, to_process);
					} else {
						// Mixed set/unset bits
						AppendBytes(state, masked_entry, to_process);
					}

					to_append -= to_process;
					appended += to_process;
					entry_offset++;
				}

				auto entry_count = to_append / ValidityMask::BITS_PER_VALUE;
				for (idx_t entry_index = 0; entry_index < entry_count; entry_index++) {
					// get the validity entry at this index
					auto validity_entry = validity.GetValidityEntry(entry_offset + entry_index);
					if (ValidityMask::AllValid(validity_entry)) {
						// All bits are set
						STATE_TYPE::HandleAllValid(state, ValidityMask::BITS_PER_VALUE);
					} else if (ValidityMask::NoneValid(validity_entry)) {
						// None of the bits are set
						STATE_TYPE::HandleNoneValid(state, ValidityMask::BITS_PER_VALUE);
					} else {
						// Mixed set/unset bits
						AppendBytes(state, validity_entry, ValidityMask::BITS_PER_VALUE);
					}
				}

				// Deal with a ragged end, when the validity entry isn't entirely used
				idx_t remainder = to_append % ValidityMask::BITS_PER_VALUE;
				if (DUCKDB_UNLIKELY(remainder != 0)) {
					auto validity_entry = validity.GetValidityEntry(entry_offset + entry_count);
					auto masked = validity_entry & ValidityUncompressed::LOWER_MASKS[remainder];
					if (masked == ValidityUncompressed::LOWER_MASKS[remainder]) {
						// All bits are set
						STATE_TYPE::HandleAllValid(state, remainder);
					} else if (masked == 0) {
						// None of the bits are set
						STATE_TYPE::HandleNoneValid(state, remainder);
					} else {
						AppendBytes(state, validity_entry, remainder);
					}
				}

				if (STATE_TYPE::Count(state) == ROARING_CONTAINER_SIZE) {
					STATE_TYPE::Flush(state);
				}
				appended += to_append;
			}
		}
	}
};

} // namespace roaring

} // namespace duckdb
