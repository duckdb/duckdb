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
#include "duckdb/storage/compression/patas/patas.hpp"

using duckdb_chimp::ByteReader;
using duckdb_chimp::ByteWriter;
using duckdb_chimp::CountZeros;

static constexpr uint32_t PATAS_GROUP_SIZE = PatasPrimitives::PATAS_GROUP_SIZE;

namespace duckdb {

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
	}

private:
	void UpdateMetadata(EXACT_TYPE previous_value, uint8_t trailing_zero, uint8_t byte_count) {
		this->previous_value = previous_value;
		trailing_zeros[index] = trailing_zero;
		byte_counts[index] = byte_count;
		index++;
	}

private:
	ByteWriter<EMPTY> byte_writer;
	uint8_t trailing_zeros[PATAS_GROUP_SIZE];
	uint8_t byte_counts[PATAS_GROUP_SIZE];
	idx_t index;
	EXACT_TYPE previous_value;
	bool first;
};

template <class EXACT_TYPE>
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
		byte_writer.WriteValue<EXACT_TYPE, EXACT_TYPE_BITSIZE>(value);
		state.first = false;
		state.UpdateMetadata(value, 0, sizeof(EXACT_TYPE));
	}

	static void StoreCompressed(EXACT_TYPE value, State &state) {
		// XOR with previous value
		EXACT_TYPE xor_result = value ^ state.previous_value;

		// Figure out the trailing zeros (max 6 bits)
		const uint8_t trailing_zero = CountZeros<EXACT_TYPE>::Trailing(xor_result);

		// Figure out the significant bytes (max 3 bits)
		const uint8_t significant_bits = EXACT_TYPE_BITSIZE - trailing_zero;
		const uint8_t significant_bytes = (significant_bits >> 3) + ((significant_bits & 7) != 0);

		state.byte_writer.WriteValue<EXACT_TYPE>(xor_result >> trailing_zero, significant_bits);
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
	void SetTrailingZeroBuffer() {
	}
	//! Set the array to read the 'byte_count' values from
	void SetByteCountBuffer() {
	}
	//! Set the array to read the significant bytes from
	void SetInputBuffer() {
		// TODO: This can probably be passed as constructor parameter
		// since the block of significant byte values is contiguous for the entire segment
	}
	//! Reset the state for a new group
	void Reset() {
	}

private:
private:
	// ByteReader byte_reader;
	// (pointer to) array of 'trailing_zero' values
	// (pointer to) array of 'byte_count' values
	// group_index - keep track of which index we're at in the group
	// EXACT_TYPE previous_value - the last value to XOR with
};

template <class EXACT_TYPE>
struct PatasDecompression {
	using State = PatasDecompressionState<EXACT_TYPE>;

	static EXACT_TYPE Load(State &state) {
		if (state.first) {
			return LoadFirst(state);
		}
		return DecompressValue(state);
	}

	static EXACT_TYPE LoadFirst(State &state) {
		// return the first value of the buffer
		// set state.first to false
	}
	static EXACT_TYPE DecompressValue(State &state) {
		// Get the trailing_zeros value for the current index
		// Get the byte_count value for the current index
		//^ these have been unpacked beforehand for the entire group

		// Read the 'byte_count' bytes size value from the buffer
		// XOR with the previous value

		// return the value
	}
};

} // namespace duckdb
