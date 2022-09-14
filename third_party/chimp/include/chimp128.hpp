#pragma once

#include <limits>
#include <stddef.h>
#include <stdint.h>
#include "chimp_utils.hpp"
#include "output_bit_stream.hpp"
#include "input_bit_stream.hpp"
#include "ring_buffer.hpp"
#include <queue>

namespace duckdb_chimp {

enum CompressionFlags {
	VALUE_IDENTICAL = 0,
	TRAILING_EXCEEDS_THRESHOLD = 1,
	LEADING_ZERO_EQUALITY = 2,
	LEADING_ZERO_LOAD = 3
};

//===--------------------------------------------------------------------===//
// Compression
//===--------------------------------------------------------------------===//

template <class Writer>
struct Chimp128CompressionState {

	Chimp128CompressionState(double* output_stream, size_t stream_size) :
		output((uint64_t*)output_stream, stream_size),
		ring_buffer(),
		previous_leading_zeros(std::numeric_limits<int32_t>::max()) {}

	void SetLeadingZeros(int32_t value = std::numeric_limits<int32_t>::max()) {
		this->previous_leading_zeros = value;
	}

	OutputBitStream<Writer>	output; //The stream to write to
	RingBuffer				ring_buffer; //! The ring buffer that holds the previous values
	int32_t					previous_leading_zeros; //! The leading zeros of the reference value
};

template <class Writer>
struct Chimp128Compression {
	using State = Chimp128CompressionState<Writer>;

	//this.previousValuesLog2 =  (int)(Math.log(previousValues) / Math.log(2));
	//! With 'previous_values' set to 128 this resolves to 7
	//! The amount of bits needed to store an index between 0-127
	static constexpr uint8_t INDEX_BITS_SIZE = 7;
	static constexpr uint8_t FLAG_BITS_SIZE = 2;
	static constexpr uint8_t FLAG_ZERO_SIZE = INDEX_BITS_SIZE + FLAG_BITS_SIZE;
	static constexpr uint8_t FLAG_ONE_SIZE = INDEX_BITS_SIZE + FLAG_BITS_SIZE + 9;
	static constexpr uint8_t BIT_SIZE = sizeof(double) * 8;

	//this.threshold = 6 + previousValuesLog2;
	static constexpr uint8_t TRAILING_ZERO_THRESHOLD = 6 + INDEX_BITS_SIZE;

	template <bool FIRST>
	static void Store(double in, State& state) {
		if (FIRST) {
			state.output.WriteValue<double, BIT_SIZE>(in);
		}
		else {
			CompressValue(in, state);
		}
	}

	static void WriteFirst(double in, State& state) {
		state.ring_buffer.Insert(in);
		state.output.WriteValue<double, BIT_SIZE>(in);
	}

	static void CompressValue(double in, State& state) {
		auto key = state.ring_buffer.Key(in);
		uint64_t xor_result;
		uint32_t previous_index;
		uint32_t trailing_zeros = 0;
		bool trailing_zeros_exceed_threshold = false;

		//! Initialize some relevant variables based on the state of the ring_buffer
		if ((int64_t)state.ring_buffer.Size() - key < RingBuffer::RING_SIZE) {
			auto current_index = state.ring_buffer.IndexOf(key);
			uint64_t tempxor_result = (uint64_t)in ^ current_index % RingBuffer::RING_SIZE;
			auto trailing_zeros = __builtin_ctzll(tempxor_result);
			trailing_zeros_exceed_threshold = trailing_zeros > TRAILING_ZERO_THRESHOLD;
			if (trailing_zeros_exceed_threshold) {
				previous_index = current_index % RingBuffer::RING_SIZE;
				xor_result = tempxor_result;
			}
			else {
				previous_index = state.ring_buffer.Size();
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
			state.output.WriteValue<uint32_t, FLAG_ZERO_SIZE>(previous_index);
			state.SetLeadingZeros();
		}
		else {
			//! Values are not identical (64)
			uint64_t leading_zeros = ChimpCompressionConstants::LEADING_ROUND[__builtin_clzll(xor_result)];

			if (trailing_zeros_exceed_threshold) {
				//! write (64 - [0|8|12|16|18|20|22|24] - [14+])(26-50 bits) and 18 bits
				int32_t significant_bits = BIT_SIZE - leading_zeros - trailing_zeros;
				//! FIXME: it feels like this would produce '11', indicating LEADING_ZERO_LOAD
				//! Instead of indicating TRAILING_EXCEEDS_THRESHOLD '01'
				auto result = 512 * (RingBuffer::RING_SIZE + previous_index) + BIT_SIZE * ChimpCompressionConstants::LEADING_REPRESENTATION[leading_zeros] + significant_bits;
				state.output.WriteValue<int32_t, FLAG_ONE_SIZE>(result);
				state.output.WriteValue<int64_t>(xor_result >> trailing_zeros, significant_bits);
				state.SetLeadingZeros();
			}
			else if (leading_zeros == state.previous_leading_zeros) {
				//! write 2 + [?] bits
				int32_t significant_bits = BIT_SIZE - leading_zeros;
				state.output.WriteValue<uint8_t, 2>(LEADING_ZERO_EQUALITY);
				state.output.WriteValue<uint64_t>(xor_result, significant_bits);
			}
			else {
				//! write 5 + [?] bits
				int32_t significant_bits = BIT_SIZE - leading_zeros;
				//! 2 bits for the flag LEADING_ZERO_LOAD ('11') + 3 bits for the leading zeros
				state.output.WriteValue<uint32_t, 5>((LEADING_ZERO_LOAD << 3) + ChimpCompressionConstants::LEADING_REPRESENTATION[leading_zeros]);
				state.output.WriteValue<uint64_t>(xor_result, significant_bits);
				state.SetLeadingZeros(leading_zeros);
			}
		}
		state.ring_buffer.Insert(in);
	}
};

//===--------------------------------------------------------------------===//
// Decompression
//===--------------------------------------------------------------------===//

#define NAN_LONG 0x7ff8000000000000L

//! Check if the returned value is NAN_LONG, in which case set the end_of_stream to true
//! And return from the method
#define RETURN_IF_EOF(x) do { \
  if ((x) == NAN_LONG) { state.end_of_stream = true; return;} \
} while (0)

struct Chimp128DecompressionState {
public:
	Chimp128DecompressionState(uint64_t* input_stream, size_t stream_size) :
		input(input_stream, stream_size),
		reference_value(0),
		initial_fill()
	{
		SetLeadingZeros();
		SetTrailingZeros();
	}

	void SetLeadingZeros(uint8_t value = std::numeric_limits<uint8_t>::max()) {
		this->zeros.leading = value;
	}
	void SetTrailingZeros(uint8_t value = 0) {
		this->zeros.trailing = value;
	}

	uint8_t LeadingZeros() const {
		return zeros.leading;
	}
	uint8_t TrailingZeros() const {
		return zeros.leading;
	}

	bool StreamEndReached() const {
		return end_of_stream;
	}

	StoredZeros zeros;
	int64_t reference_value = 0;
	RingBuffer	ring_buffer;

	bool end_of_stream = false;
	InputBitStream<uint64_t> input;
	std::queue<uint64_t> output_values;
	int32_t initial_fill;
};

struct StoredZeros {
	uint8_t leading;
	uint8_t trailing;
};

struct Chimp128Decompression {
public:
	//! Index value is between 1 and 127, so it's saved in 7 bits at most
	static constexpr uint8_t INDEX_BITS_SIZE = 7;
	static constexpr uint8_t LEADING_BITS_SIZE = 3;
	static constexpr uint8_t SIGNIFICANT_BITS_SIZE = 6;
	static constexpr uint8_t INITIAL_FILL = INDEX_BITS_SIZE + LEADING_BITS_SIZE + SIGNIFICANT_BITS_SIZE;
	static constexpr uint8_t BIT_SIZE = sizeof(double) * 8;

	static constexpr uint8_t INDEX_MASK = ((uint8_t)1 << INDEX_BITS_SIZE) - 1;
	static constexpr uint8_t LEADING_MASK = ((uint8_t)1 << LEADING_BITS_SIZE) - 1;
	static constexpr uint8_t SIGNIFICANT_MASK = ((uint8_t)1 << SIGNIFICANT_BITS_SIZE) - 1;

	static constexpr uint8_t INDEX_SHIFT_AMOUNT = INITIAL_FILL - INDEX_BITS_SIZE;
	static constexpr uint8_t LEADING_SHIFT_AMOUNT = INDEX_SHIFT_AMOUNT - LEADING_BITS_SIZE;

	//|----------------|	//! INITIAL_FILL(16) bits
	// IIIIIII				//! Index (7 bits, shifted by 9)
	//        LLL			//! LeadingZeros (3 bits, shifted by 6)
	//           SSSSSS 	//! SignificantBits (6 bits)
	static void UnpackPackedData(uint16_t packed_data, uint16_t& index, uint16_t leading_zeros, uint16_t significant_bits) {
		index = packed_data >> INDEX_SHIFT_AMOUNT & INDEX_MASK;
		leading_zeros = packed_data >> LEADING_SHIFT_AMOUNT & LEADING_MASK;
		significant_bits = packed_data & SIGNIFICANT_MASK;
	}

	template <bool FIRST = false>
	static void Load(Chimp128DecompressionState& state) {
		if (FIRST) {
			LoadFirst(state);
		}
		else {
			DecompressValue(state);
		}
	}

	static void LoadFirst(Chimp128DecompressionState& state) {
		uint64_t value = state.input.ReadValue<uint64_t>();
		state.output_values.push(value);
		state.ring_buffer.Insert(value);
		if (value == NAN_LONG) {
			state.end_of_stream = true;
		}
	}

	static void DecompressValue(Chimp128DecompressionState& state) {
		auto flag = state.input.ReadValue<uint8_t, 2>();
		uint64_t value;
		switch (flag) {
		case LEADING_ZERO_LOAD:
			auto deserialized_leading_zeros = state.input.ReadValue<uint8_t, LEADING_BITS_SIZE>();
			state.SetLeadingZeros(ChimpDecompressionConstants::LEADING_REPRESENTATION[deserialized_leading_zeros]);
            value = state.input.ReadValue<uint64_t>(BIT_SIZE - state.LeadingZeros());
            value ^= state.reference_value;
			RETURN_IF_EOF(value);
			state.reference_value = value;
			state.ring_buffer.Insert(value);
			break;
		case LEADING_ZERO_EQUALITY:
			value = state.input.ReadValue<uint64_t>(BIT_SIZE - state.LeadingZeros());
			value ^= state.reference_value;
			RETURN_IF_EOF(value);
			state.reference_value = value;
			state.ring_buffer.Insert(value);
			break;
		case TRAILING_EXCEEDS_THRESHOLD:
			uint16_t index, leading_zeros, significant_bits;
			uint16_t temp = state.input.ReadValue<uint64_t, INITIAL_FILL>();
			UnpackPackedData(temp, index, leading_zeros, significant_bits);
			state.SetLeadingZeros(ChimpDecompressionConstants::LEADING_REPRESENTATION[leading_zeros]);
			state.reference_value = state.ring_buffer.Value(index);
			if (significant_bits == 0) {
				significant_bits = BIT_SIZE;
			}
			state.SetTrailingZeros(BIT_SIZE - significant_bits - state.LeadingZeros());
			value = state.input.ReadValue<uint64_t>(BIT_SIZE - state.LeadingZeros() - state.TrailingZeros());
			value <<= state.TrailingZeros();
			value ^= state.reference_value;
			RETURN_IF_EOF(value);
			state.reference_value = value;
			state.ring_buffer.Insert(value);
			break;
		case VALUE_IDENTICAL:
			//! Value is identical to previous value
			auto index = state.input.ReadValue<uint8_t>(INDEX_BITS_SIZE);
			value = state.ring_buffer.Value(index);
			state.reference_value = value;
			state.ring_buffer.Insert(state.reference_value);
		default:
			//! This should not happen, value isn't properly (de)serialized if it does
			assert(flag != 0);
		}
	}

};

} //namespace duckdb_chimp
