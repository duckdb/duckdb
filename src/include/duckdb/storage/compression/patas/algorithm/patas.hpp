//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/compression/patas/algorithm/patas.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/compression/chimp/algorithm/byte_writer.hpp"
#include "duckdb/storage/compression/chimp/algorithm/byte_reader.hpp"
#include "duckdb/storage/compression/chimp/algorithm/chimp_utils.hpp"
#include "duckdb/storage/compression/patas/shared.hpp"

static constexpr uint32_t PATAS_GROUP_SIZE = duckdb::PatasPrimitives::PATAS_GROUP_SIZE;

namespace duckdb {

namespace patas {

using duckdb_chimp::ByteReader;
using duckdb_chimp::ByteWriter;
using duckdb_chimp::CountZeros;

template <class EXACT_TYPE, bool EMPTY>
class PatasCompressionState {
public:
	PatasCompressionState() : index(0), previous_value(0), first(true) {
	}

public:
	void Reset() {
		index = 0;
		previous_value = 0;
		first = true;
	}
	void SetOutputBuffer(uint8_t *output) {
		byte_writer.SetStream(output);
		Reset();
	}
	idx_t Index() const {
		return index;
	}

public:
	void UpdateMetadata(EXACT_TYPE previous_value, uint8_t trailing_zero, uint8_t byte_count) {
		this->previous_value = previous_value;
		trailing_zeros[index] = trailing_zero;
		byte_counts[index] = byte_count;
		index++;
	}

public:
	ByteWriter<EMPTY> byte_writer;
	uint8_t trailing_zeros[PATAS_GROUP_SIZE];
	uint8_t byte_counts[PATAS_GROUP_SIZE];
	idx_t index;
	EXACT_TYPE previous_value;
	bool first;
};

template <class EXACT_TYPE, bool EMPTY>
struct PatasCompression {
	using State = PatasCompressionState<EXACT_TYPE, EMPTY>;
	static constexpr uint8_t EXACT_TYPE_BITSIZE = sizeof(EXACT_TYPE) * 8;

	static void Store(EXACT_TYPE value, State &state) {
		if (state.first) {
			StoreFirst(value, state);
		} else {
			StoreCompressed(value, state);
		}
	}

	static void StoreFirst(EXACT_TYPE value, State &state) {
		// write first value, uncompressed
		state.byte_writer.template WriteValue<EXACT_TYPE, EXACT_TYPE_BITSIZE>(value);
		state.first = false;
		state.UpdateMetadata(value, 0, sizeof(EXACT_TYPE));
	}

	static void StoreCompressed(EXACT_TYPE value, State &state) {
		// XOR with previous value
		EXACT_TYPE xor_result = value ^ state.previous_value;

		// Figure out the trailing zeros (max 6 bits)
		const uint8_t trailing_zero = CountZeros<EXACT_TYPE>::Trailing(xor_result);

		const bool is_equal = xor_result == 0;

		// Figure out the significant bytes (max 3 bits)
		const uint8_t significant_bits = (EXACT_TYPE_BITSIZE - trailing_zero) + is_equal;
		const uint8_t significant_bytes = (significant_bits >> 3) + ((significant_bits & 7) != 0);

		// Avoid an invalid shift error when xor_result is 0
		state.byte_writer.template WriteValue<EXACT_TYPE>(xor_result >> (trailing_zero - is_equal),
		                                                  significant_bits - is_equal);
		// We store equal as: 0 bytes of data, byte_count: 1, trailing_zeros: 0
		state.UpdateMetadata(value, trailing_zero, significant_bytes);
	}
};

// Decompression

template <class EXACT_TYPE>
class PatasDecompressionState {
public:
	PatasDecompressionState() {
	}

public:
	//! Set the array to read the 'trailing_zero' values from
	void SetTrailingZeroBuffer(uint8_t *buffer) {
		trailing_zeros = buffer;
	}
	//! Set the array to read the 'byte_count' values from
	void SetByteCountBuffer(uint8_t *buffer) {
		byte_counts = buffer;
	}
	//! Set the array to read the significant bytes from
	void SetInputBuffer(uint8_t *buffer) {
		// TODO: This can probably be passed as constructor parameter
		// since the block of significant byte values is contiguous for the entire segment
		byte_reader.SetStream(buffer);
	}
	//! Reset the state for a new group
	void Reset() {
		group_index = 0;
		previous_value = 0;
	}
	ByteReader byte_reader;
	uint8_t *trailing_zeros;
	uint8_t *byte_counts;
	idx_t group_index;
	EXACT_TYPE previous_value;
};

template <class EXACT_TYPE>
struct PatasDecompression {
	using State = PatasDecompressionState<EXACT_TYPE>;

	static EXACT_TYPE Load(State &state) {
		if (state.group_index == 0) {
			return LoadFirst(state);
		}
		return DecompressValue(state);
	}

	static EXACT_TYPE LoadFirst(State &state) {
		// return the first value of the buffer
		// set state.first to false
		D_ASSERT(state.group_index == 0);
		EXACT_TYPE result = state.byte_reader.template ReadValue<EXACT_TYPE>(sizeof(EXACT_TYPE) * 8);
		state.previous_value = result;
		state.group_index++;
		return result;
	}

	static EXACT_TYPE DecompressValue(State &state) {
		D_ASSERT(state.group_index != 0);
		// Get the trailing_zeros value for the current index
		// Get the byte_count value for the current index

		auto byte_count = state.byte_counts[state.group_index];
		D_ASSERT(byte_count <= sizeof(EXACT_TYPE));
		auto trailing_zeros = state.trailing_zeros[state.group_index];
		D_ASSERT(trailing_zeros <= 64);

		// Full bytes is stored as 0
		byte_count += (sizeof(EXACT_TYPE) * (byte_count == 0));
		// Equal is stored as byte_count: 1, trailing_zeros: 0
		byte_count -= (byte_count == 1 && trailing_zeros == 0);

		EXACT_TYPE result = state.byte_reader.template ReadValue<EXACT_TYPE>(byte_count * 8);
		result <<= trailing_zeros;
		result ^= state.previous_value;

		state.group_index++;
		state.previous_value = result;
		return result;
	}
};

} // namespace patas

} // namespace duckdb
