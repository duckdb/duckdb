//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/compression/patas/patas.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

namespace duckdb {

template <class FLOATING_TYPE, bool EMPTY>
class PatasCompressionState {
public:
public:
private:
private:
};

template <class FLOATING_TYPE>
struct PatasCompression {
	using State = PatasCompressionState<FLOATING_TYPE, EMPTY>;

	static void Store(FLOATING_TYPE value, State &state) {
		if (state.first) {
			StoreFirst(value, state);
		} else {
			StoreCompressed(value, state);
		}
	}

	static void StoreFirst(FLOATING_TYPE value, State &state) {
		// write first value, uncompressed
	}

	static void StoreCompressed(FLOATING_TYPE value, State &state) {
		// XOR with previous value

		// Figure out the trailing zeros (max 6 bits)

		// Figure out the significant bytes (max 3 bits)

		// ^ store these in a temporary buffer, compress them with FOR
		// Every BITPACKING_GROUP_SIZE, store them to disk

		// Write the significant bytes to disk
	}
};

// Decompression (happens per segment)

template <class FLOATING_TYPE>
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
	// FLOATING_TYPE previous_value - the last value to XOR with
};

template <class FLOATING_TYPE>
struct PatasDecompression {
	using State = PatasDecompressionState<FLOATING_TYPE>;

	static FLOATING_TYPE Load(State &state) {
		if (state.first) {
			return LoadFirst(state);
		}
		return DecompressValue(state);
	}

	static FLOATING_TYPE LoadFirst(State &state) {
		// return the first value of the buffer
		// set state.first to false
	}
	static FLOATING_TYPE DecompressValue(State &state) {
		// Get the trailing_zeros value for the current index
		// Get the byte_count value for the current index
		//^ these have been unpacked beforehand for the entire group

		// Read the 'byte_count' bytes size value from the buffer
		// XOR with the previous value

		// return the value
	}
};

} // namespace duckdb
