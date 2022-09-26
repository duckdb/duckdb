//===----------------------------------------------------------------------===//
//                         DuckDB
//
// third_party/chimp/include/chimp128.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <limits>
#include <stddef.h>
#include <stdint.h>
#include "chimp_utils.hpp"
#include "output_bit_stream.hpp"
#include "bit_reader_optimized.hpp"
#include "ring_buffer.hpp"
#include <assert.h>

namespace duckdb_chimp {

using bit_index_t = uint32_t;

enum CompressionFlags {
	VALUE_IDENTICAL = 0,
	TRAILING_EXCEEDS_THRESHOLD = 1,
	LEADING_ZERO_EQUALITY = 2,
	LEADING_ZERO_LOAD = 3
};

//===--------------------------------------------------------------------===//
// Compression
//===--------------------------------------------------------------------===//

template <bool EMPTY>
struct Chimp128CompressionState {

	Chimp128CompressionState() :
		ring_buffer(),
		previous_leading_zeros(std::numeric_limits<uint8_t>::max()) {}

	inline void SetLeadingZeros(int32_t value = std::numeric_limits<uint8_t>::max()) {
		this->previous_leading_zeros = value;
	}

	void	SetOutputBuffer(uint8_t* stream) {
		output.SetStream(stream);
	}

	size_t CompressedSize() const {
		return output.BitsWritten();
	}

	void Reset() {
		first = true;
		ring_buffer.Reset();
		SetLeadingZeros();
	}

	OutputBitStream<EMPTY>					output; //The stream to write to
	RingBuffer								ring_buffer; //! The ring buffer that holds the previous values
	uint8_t									previous_leading_zeros; //! The leading zeros of the reference value
	bool									first = true;
};

template <bool EMPTY>
class Chimp128Compression {
public:
	using State = Chimp128CompressionState<EMPTY>;
	static constexpr uint8_t FLAG_MASK = (1 << 2) - 1;

	//this.previousValuesLog2 =  (int)(Math.log(previousValues) / Math.log(2));
	//! With 'previous_values' set to 128 this resolves to 7
	//! The amount of bits needed to store an index between 0-127
	static constexpr uint8_t INDEX_BITS_SIZE = 7;
	static constexpr uint8_t FLAG_BITS_SIZE = 2;
	static constexpr uint8_t FLAG_ZERO_SIZE = INDEX_BITS_SIZE + FLAG_BITS_SIZE;
	static constexpr uint8_t FLAG_ONE_SIZE = INDEX_BITS_SIZE + FLAG_BITS_SIZE + 9;
	static constexpr uint8_t BIT_SIZE = sizeof(uint64_t) * 8;

	//this.threshold = 6 + previousValuesLog2;
	static constexpr uint8_t TRAILING_ZERO_THRESHOLD = 6 + INDEX_BITS_SIZE;

	static void Store(uint64_t in, State& state) {
		if (state.first) {
			WriteFirst(in, state);
		}
		else {
			CompressValue(in, state);
		}
	}

	//! Write the content of the bit buffer to the stream
	static void Flush(State& state) {
		if (!EMPTY) {
			state.output.Flush();
		}
	}

	static void WriteFirst(uint64_t in, State& state) {
		state.ring_buffer.template Insert<true>(in);
		state.output.template WriteValue<uint64_t, BIT_SIZE>(in);
		state.first = false;
	}

	static void CompressValue(uint64_t in, State& state) {
		auto key = state.ring_buffer.Key(in);
		uint64_t xor_result;
		uint32_t previous_index;
		uint32_t trailing_zeros = 0;
		bool trailing_zeros_exceed_threshold = false;

		//! Find the reference value to use when compressing the current value
		if (((int64_t)state.ring_buffer.Size() - (int64_t)key) < (int64_t)RingBuffer::RING_SIZE) {
			auto current_index = state.ring_buffer.IndexOf(key);
			if (current_index > state.ring_buffer.Size()) {
				current_index = 0;
			}
			auto reference_value = state.ring_buffer.Value(current_index % RingBuffer::RING_SIZE);
			uint64_t tempxor_result = (uint64_t)in ^ reference_value;
			trailing_zeros = __builtin_ctzll(tempxor_result);
			trailing_zeros_exceed_threshold = trailing_zeros > TRAILING_ZERO_THRESHOLD;
			if (trailing_zeros_exceed_threshold) {
				previous_index = current_index % RingBuffer::RING_SIZE;
				xor_result = tempxor_result;
			}
			else {
				previous_index = state.ring_buffer.Size() % RingBuffer::RING_SIZE;
				xor_result = (uint64_t)in ^ state.ring_buffer.Value(previous_index);
			}
		}
		else {
			previous_index = state.ring_buffer.Size() % RingBuffer::RING_SIZE;
			xor_result = (uint64_t)in ^ state.ring_buffer.Value(previous_index);
		}


		//! Compress the value
		if (xor_result == 0) {
			//! The two values are identical (write 9 bits)
			//! 2 bits for the flag VALUE_IDENTICAL ('00') + 7 bits for the referenced index value
			state.output.template WriteValue<uint32_t, FLAG_ZERO_SIZE>(previous_index);
			state.SetLeadingZeros();
		}
		else {
			//! Values are not identical (64)
			auto leading_zeros_raw = __builtin_clzll(xor_result);
			uint8_t leading_zeros = ChimpCompressionConstants::LEADING_ROUND[leading_zeros_raw];

			if (trailing_zeros_exceed_threshold) {
				//! write (64 - [0|8|12|16|18|20|22|24] - [14+])(26-50 bits) and 18 bits
				uint32_t significant_bits = BIT_SIZE - leading_zeros - trailing_zeros;
				//! FIXME: it feels like this would produce '11', indicating LEADING_ZERO_LOAD
				//! Instead of indicating TRAILING_EXCEEDS_THRESHOLD '01'
				auto result = 512U * (RingBuffer::RING_SIZE + previous_index) + BIT_SIZE * ChimpCompressionConstants::LEADING_REPRESENTATION[leading_zeros] + significant_bits;
				state.output.template WriteValue<uint32_t, FLAG_ONE_SIZE>(result);
				state.output.template WriteValue<uint64_t>(xor_result >> trailing_zeros, significant_bits);
				state.SetLeadingZeros();
			}
			else if (leading_zeros == state.previous_leading_zeros) {
				//! write 2 + [?] bits
				int32_t significant_bits = BIT_SIZE - leading_zeros;
				state.output.template WriteValue<uint8_t, 2>(LEADING_ZERO_EQUALITY);
				state.output.template WriteValue<uint64_t>(xor_result, significant_bits);
			}
			else {
				int32_t significant_bits = BIT_SIZE - leading_zeros;
				//! 2 bits for the flag LEADING_ZERO_LOAD ('11') + 3 bits for the leading zeros
				state.output.template WriteValue<uint32_t, 5>(((uint8_t)LEADING_ZERO_LOAD << 3) + ChimpCompressionConstants::LEADING_REPRESENTATION[leading_zeros]);
				state.output.template WriteValue<uint64_t>(xor_result, significant_bits);
				state.SetLeadingZeros(leading_zeros);
			}
		}
		// Byte-align every value we write to the output
		state.output.ByteAlign();
		state.ring_buffer.Insert(in);
	}
};

//===--------------------------------------------------------------------===//
// Decompression
//===--------------------------------------------------------------------===//

struct Chimp128DecompressionState {
public:
	Chimp128DecompressionState() :
		reference_value(0),
		first(true)
	{
		SetLeadingZeros();
		SetTrailingZeros();
	}

	void Reset() {
		SetLeadingZeros();
		SetTrailingZeros();
		reference_value = 0;
		ring_buffer.Reset();
		first = true;
	}

	inline void SetLeadingZeros(const uint8_t &value) {
		leading_zeros = value;
	}

	inline void SetLeadingZeros(uint8_t &&value = std::numeric_limits<uint8_t>::max()) {
		leading_zeros = value;
	}
	inline void SetTrailingZeros(uint8_t &&value = 0) {
		assert(value <= sizeof(uint64_t) * 8);
		trailing_zeros = value;
	}

	const uint8_t &LeadingZeros() const {
		return leading_zeros;
	}
	const uint8_t &TrailingZeros() const {
		return trailing_zeros;
	}

	BitReader input;
	uint8_t leading_zeros;
	uint8_t trailing_zeros;
	uint64_t reference_value = 0;
	RingBuffer	ring_buffer;

	bool first;
};

template <class RETURN_TYPE>
struct Chimp128Decompression {
public:
	//! Index value is between 1 and 127, so it's saved in 7 bits at most
	static constexpr uint8_t INDEX_BITS_SIZE = 7;
	static constexpr uint8_t LEADING_BITS_SIZE = 3;
	static constexpr uint8_t SIGNIFICANT_BITS_SIZE = 6;
	static constexpr uint8_t INITIAL_FILL = INDEX_BITS_SIZE + LEADING_BITS_SIZE + SIGNIFICANT_BITS_SIZE;
	static constexpr uint8_t BIT_SIZE = sizeof(uint64_t) * 8;

	static constexpr uint8_t INDEX_MASK = ((uint8_t)1 << INDEX_BITS_SIZE) - 1;
	static constexpr uint8_t LEADING_MASK = ((uint8_t)1 << LEADING_BITS_SIZE) - 1;
	static constexpr uint8_t SIGNIFICANT_MASK = ((uint8_t)1 << SIGNIFICANT_BITS_SIZE) - 1;

	static constexpr uint8_t INDEX_SHIFT_AMOUNT = INITIAL_FILL - INDEX_BITS_SIZE;
	static constexpr uint8_t LEADING_SHIFT_AMOUNT = INDEX_SHIFT_AMOUNT - LEADING_BITS_SIZE;

	//|----------------|	//! INITIAL_FILL(16) bits
	// IIIIIII				//! Index (7 bits, shifted by 9)
	//        LLL			//! LeadingZeros (3 bits, shifted by 6)
	//           SSSSSS 	//! SignificantBits (6 bits)
	static inline void UnpackPackedData(uint16_t packed_data, uint16_t& index, uint8_t& leading_zeros, uint16_t& significant_bits) {
		index = packed_data >> INDEX_SHIFT_AMOUNT & INDEX_MASK;
		leading_zeros = packed_data >> LEADING_SHIFT_AMOUNT & LEADING_MASK;
		significant_bits = packed_data & SIGNIFICANT_MASK;
	}

	static inline RETURN_TYPE Load(RETURN_TYPE &value, Chimp128DecompressionState& state) {
		if (state.first) {
			return LoadFirst(value, state);
		}
		else {
			return DecompressValue(value, state);
		}
	}

	static inline bool LoadFirst(RETURN_TYPE &value, Chimp128DecompressionState& state) {
		value = state.input.template ReadValue<RETURN_TYPE, (sizeof(RETURN_TYPE) * 8)>();
		state.ring_buffer.Insert<true>(value);
		state.first = false;
		state.reference_value = value;
		return true;
	}

	static inline bool DecompressValue(RETURN_TYPE &value, Chimp128DecompressionState& state) {
		alignas(8) static constexpr uint8_t LEADING_REPRESENTATION[] = {
			0, 8, 12, 16, 18, 20, 22, 24
		};

		auto flag = state.input.template ReadValue<uint8_t, 2>();
		switch (flag) {
		case VALUE_IDENTICAL: {
			//! Value is identical to previous value
			auto index = state.input.template ReadValue<uint8_t, INDEX_BITS_SIZE>();
			value = state.ring_buffer.Value(index);
			break;
		}
		case TRAILING_EXCEEDS_THRESHOLD: {
			uint16_t index;
			uint8_t leading_zeros;
			uint16_t significant_bits;
			uint16_t temp = state.input.template ReadValue<uint64_t, INITIAL_FILL>();
			UnpackPackedData(temp, index, leading_zeros, significant_bits);
			state.leading_zeros = LEADING_REPRESENTATION[leading_zeros];
			if (significant_bits == 0) {
				significant_bits = 64;
			}
			state.SetTrailingZeros(BIT_SIZE - significant_bits - state.LeadingZeros());
			value = state.input.template ReadValue<uint64_t>(BIT_SIZE - state.LeadingZeros() - state.TrailingZeros());
			value <<= state.TrailingZeros();
			value ^= state.ring_buffer.Value(index);
			break;
		}
		case LEADING_ZERO_EQUALITY: {
			value = state.input.template ReadValue<uint64_t>(BIT_SIZE - state.LeadingZeros());
			value ^= state.reference_value;
			break;
		}
		case LEADING_ZERO_LOAD: {
			const auto deserialized_leading_zeros = state.input.template ReadValue<uint8_t>(LEADING_BITS_SIZE);
			state.SetLeadingZeros(LEADING_REPRESENTATION[deserialized_leading_zeros]);
            value = state.input.template ReadValue<uint64_t>(BIT_SIZE - state.LeadingZeros());
            value ^= state.reference_value;
			break;
		}
		default:
			//! This should not happen, value isn't properly (de)serialized if it does
			assert(1 == 0);
		}
		state.reference_value = value;
		state.ring_buffer.Insert(value);
		return true;
	}

};

} //namespace duckdb_chimp
