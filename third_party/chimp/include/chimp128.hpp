#pragma once

#include <limits>
#include <stddef.h>
#include <stdint.h>
#include "chimp_utils.hpp"
#include "output_bit_stream.hpp"
#include "input_bit_stream.hpp"
#include "ring_buffer.hpp"
#include <queue>
#include <memory>

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

template <bool EMPTY>
struct Chimp128CompressionState {

	Chimp128CompressionState() :
		ring_buffer(),
		previous_leading_zeros(std::numeric_limits<uint8_t>::max()) {}

	void SetLeadingZeros(int32_t value = std::numeric_limits<uint8_t>::max()) {
		this->previous_leading_zeros = value;
	}

	void	SetOutputBuffer(uint8_t* stream) {
		output = std::unique_ptr<OutputBitStream<EMPTY>>(new OutputBitStream<EMPTY>(stream));
	}

	size_t CompressedSize() const {
		return output->BitsWritten();
	}

	std::unique_ptr<OutputBitStream<EMPTY>>	output; //The stream to write to
	RingBuffer								ring_buffer; //! The ring buffer that holds the previous values
	uint8_t									previous_leading_zeros; //! The leading zeros of the reference value
};

template <bool EMPTY>
class Chimp128Compression {
public:
	using State = Chimp128CompressionState<EMPTY>;

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

	template <bool FIRST>
	static void Store(uint64_t in, State& state) {
		if (FIRST) {
			WriteFirst(in, state);
		}
		else {
			CompressValue(in, state);
		}
	}

	//! Write the content of the bit buffer to the stream
	static void Flush(State& state) {
		if (!EMPTY) {
			state.output->Flush();
		}
	}

	static void WriteFirst(uint64_t in, State& state) {
		//printf("First value: %f\n", in);
		state.ring_buffer.template Insert<true>(in);
		state.output->template WriteValue<uint64_t, BIT_SIZE>(in);
	}

	static void CompressValue(uint64_t in, State& state) {
		auto key = state.ring_buffer.Key(in);
		printf("key: %llu\n", key);
		uint64_t xor_result;
		uint32_t previous_index;
		uint32_t trailing_zeros = 0;
		bool trailing_zeros_exceed_threshold = false;

		//! Initialize some relevant variables based on the state of the ring_buffer
		if (((int64_t)state.ring_buffer.Size() - (int64_t)key) < (int64_t)RingBuffer::RING_SIZE) {
			auto current_index = state.ring_buffer.IndexOf(key);
			printf("current_index: %llu\n", current_index);
			auto reference_value = state.ring_buffer.Value(current_index % RingBuffer::RING_SIZE);
			printf("reference_value: %llu\n", reference_value);
			uint64_t tempxor_result = (uint64_t)in ^ reference_value;
			trailing_zeros = __builtin_ctzll(tempxor_result);
			printf("trailing_zeros: %llu\n", trailing_zeros);
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
			printf("IDENTICAL\n");
			//! The two values are identical (write 9 bits)
			//! 2 bits for the flag VALUE_IDENTICAL ('00') + 7 bits for the referenced index value
			state.output->template WriteValue<uint32_t, FLAG_ZERO_SIZE>(previous_index);
			state.SetLeadingZeros();
		}
		else {
			//! Values are not identical (64)
			printf("Xor: %llu\n", xor_result);
			auto leading_zeros_raw = __builtin_clzll(xor_result);
			printf("leading_zeros_raw = %d\n", leading_zeros_raw);
			uint8_t leading_zeros = ChimpCompressionConstants::LEADING_ROUND[leading_zeros_raw];
			printf("leading_zeros = %d\n", leading_zeros);

			if (trailing_zeros_exceed_threshold) {
				printf("EXCEEDS THRESHOLD\n");
				//! write (64 - [0|8|12|16|18|20|22|24] - [14+])(26-50 bits) and 18 bits
				uint32_t significant_bits = BIT_SIZE - leading_zeros - trailing_zeros;
				//! FIXME: it feels like this would produce '11', indicating LEADING_ZERO_LOAD
				//! Instead of indicating TRAILING_EXCEEDS_THRESHOLD '01'
				auto result = 512U * (RingBuffer::RING_SIZE + previous_index) + BIT_SIZE * ChimpCompressionConstants::LEADING_REPRESENTATION[leading_zeros] + significant_bits;
				state.output->template WriteValue<uint32_t, FLAG_ONE_SIZE>(result);
				state.output->template WriteValue<uint64_t>(xor_result >> trailing_zeros, significant_bits);
				state.SetLeadingZeros();
			}
			else if (leading_zeros == state.previous_leading_zeros) {
				printf("LEADING_ZERO_EQUALITY\n");
				//! write 2 + [?] bits
				int32_t significant_bits = BIT_SIZE - leading_zeros;
				state.output->template WriteValue<uint8_t, 2>(LEADING_ZERO_EQUALITY);
				state.output->template WriteValue<uint64_t>(xor_result, significant_bits);
			}
			else {
				printf("OUTLIER\n");
				//! write 5 + [?] bits
				int32_t significant_bits = BIT_SIZE - leading_zeros;
				//! 2 bits for the flag LEADING_ZERO_LOAD ('11') + 3 bits for the leading zeros
				state.output->template WriteValue<uint32_t, 5>(((uint8_t)LEADING_ZERO_LOAD << 3) + ChimpCompressionConstants::LEADING_REPRESENTATION[leading_zeros]);
				state.output->template WriteValue<uint64_t>(xor_result, significant_bits);
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
  if ((x) == NAN_LONG) { state.end_of_stream = true; return false;} \
} while (0)

struct StoredZeros {
	uint8_t leading;
	uint8_t trailing;
};

struct Chimp128DecompressionState {
public:
	Chimp128DecompressionState(uint64_t* input_stream, size_t stream_size) :
		input((uint8_t*)input_stream, stream_size),
		reference_value(0),
		initial_fill(),
		first(true)
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
		return zeros.trailing;
	}

	bool StreamEndReached() const {
		return end_of_stream;
	}

	InputBitStream input;
	StoredZeros zeros;
	uint64_t reference_value = 0;
	RingBuffer	ring_buffer;

	bool end_of_stream = false;
	int32_t initial_fill;
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
	static void UnpackPackedData(uint16_t packed_data, uint16_t& index, uint16_t& leading_zeros, uint16_t& significant_bits) {
		index = packed_data >> INDEX_SHIFT_AMOUNT & INDEX_MASK;
		leading_zeros = packed_data >> LEADING_SHIFT_AMOUNT & LEADING_MASK;
		significant_bits = packed_data & SIGNIFICANT_MASK;
		printf("unpacked.index: %hu | unpacked.leading_zeros: %hu | unpacked.significant_bits: %hu\n", index, leading_zeros, significant_bits);
	}

	static RETURN_TYPE Load(RETURN_TYPE &value, Chimp128DecompressionState& state) {
		if (state.first) {
			return LoadFirst(value, state);
		}
		else {
			return DecompressValue(value, state);
		}
	}

	static bool LoadFirst(RETURN_TYPE &value, Chimp128DecompressionState& state) {
		value = state.input.template ReadValue<RETURN_TYPE>();
		printf("value: %lluu\n", value);
		state.ring_buffer.Insert<true>(value);
		state.first = false;
		state.reference_value = value;
		RETURN_IF_EOF(value);
		return true;
	}

	static bool DecompressValue(RETURN_TYPE &value, Chimp128DecompressionState& state) {
		auto flag = state.input.template ReadValue<uint8_t>(2);
		printf("flag: %u\n", (uint32_t)flag);
		switch (flag) {
		case LEADING_ZERO_LOAD: {
			printf("LEADING ZERO LOAD\n");
			auto deserialized_leading_zeros = state.input.template ReadValue<uint8_t>(LEADING_BITS_SIZE);
			printf("deserialized_leading_zeros: %u\n", (uint32_t)deserialized_leading_zeros);
			state.SetLeadingZeros(ChimpDecompressionConstants::LEADING_REPRESENTATION[deserialized_leading_zeros]);
			printf("leading_zeros: %u\n", state.LeadingZeros());
            value = state.input.template ReadValue<uint64_t>(BIT_SIZE - state.LeadingZeros());
			printf("value: %llu\n", value);
            value ^= state.reference_value;
			RETURN_IF_EOF(value);
			break;
		}
		case LEADING_ZERO_EQUALITY: {
			printf("LEADING ZERO EQUALITY\n");
			value = state.input.template ReadValue<uint64_t>(BIT_SIZE - state.LeadingZeros());
			printf("value: %llu\n", value);
			value ^= state.reference_value;
			RETURN_IF_EOF(value);
			break;
		}
		case TRAILING_EXCEEDS_THRESHOLD: {
			printf("TRAILING EXCEEDS THRESHOLD\n");
			uint16_t index, leading_zeros, significant_bits;
			uint16_t temp = state.input.template ReadValue<uint64_t>(INITIAL_FILL);
			printf("temp: %lu\n", temp);
			UnpackPackedData(temp, index, leading_zeros, significant_bits);
			state.SetLeadingZeros(ChimpDecompressionConstants::LEADING_REPRESENTATION[leading_zeros]);
			state.reference_value = state.ring_buffer.Value(index);
			if (significant_bits == 0) {
				significant_bits = BIT_SIZE;
			}
			state.SetTrailingZeros(BIT_SIZE - significant_bits - state.LeadingZeros());
			auto bits_to_read = BIT_SIZE - state.LeadingZeros() - state.TrailingZeros();
			printf("stored_leading_zeros: %u\n", state.LeadingZeros());
			printf("stored_trailing_zeros: %u\n", state.TrailingZeros());
			printf("read_amount: %u\n", (uint32_t)bits_to_read);
			value = state.input.template ReadValue<uint64_t>(bits_to_read);
			printf("value: %llu\n", value);
			value <<= state.TrailingZeros();
			value ^= state.reference_value;
			RETURN_IF_EOF(value);
			break;
		}
		case VALUE_IDENTICAL: {
			printf("VALUE IDENTICAL\n");
			//! Value is identical to previous value
			auto index = state.input.template ReadValue<uint8_t>(INDEX_BITS_SIZE);
			printf("index: %u\n", (uint32_t)index);
			value = state.ring_buffer.Value(index);
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
