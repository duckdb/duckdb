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
		const uint8_t leading_zero = CountZeros<EXACT_TYPE>::Leading(xor_result);

		const bool is_equal = xor_result == 0;

		// Figure out the significant bytes (max 3 bits)
		const uint8_t significant_bits = is_equal + (!is_equal * (EXACT_TYPE_BITSIZE - trailing_zero - leading_zero));
		const uint8_t significant_bytes = (significant_bits >> 3) + ((significant_bits & 7) != 0);

		// Avoid an invalid shift error when xor_result is 0
		state.byte_writer.template WriteValue<EXACT_TYPE>(xor_result >> (trailing_zero - is_equal), significant_bits);
		state.UpdateMetadata(value, trailing_zero, significant_bytes);
		// if (!EMPTY) {
		//	printf("COMPRESS: byte_count: %u | trailing_zero: %u\n", (uint32_t)significant_bytes,
		//	       (uint32_t)trailing_zero);
		// }
	}
};

// Decompression

template <class EXACT_TYPE>
class PatasDecompressionState {
public:
	PatasDecompressionState() {
	}

public:
	//! Set the array to read the significant bytes from
	void SetInputBuffer(const uint8_t *buffer) {
		// TODO: This can probably be passed as constructor parameter
		// since the block of significant byte values is contiguous for the entire segment
		byte_reader.SetStream(buffer);
	}
	//! Reset the state for a new group
	void Reset() {
		previous_value = 0;
	}
	ByteReader byte_reader;
	EXACT_TYPE previous_value;
};

template <class EXACT_TYPE>
struct PatasDecompression {
	using State = PatasDecompressionState<EXACT_TYPE>;

	static inline EXACT_TYPE Load(State &state, idx_t index, uint8_t byte_counts[], uint8_t trailing_zeros[]) {
		return DecompressValue(state, index, byte_counts, trailing_zeros);
	}

	static inline EXACT_TYPE DecompressValue(State &state, idx_t index, uint8_t byte_counts[],
	                                         uint8_t trailing_zeros[]) {
		const EXACT_TYPE result =
		    (state.byte_reader.template ReadValue<EXACT_TYPE>(byte_counts[index]) << trailing_zeros[index]) ^
		    state.previous_value;

		state.previous_value = result;
		return result;
	}
};

} // namespace patas

} // namespace duckdb
