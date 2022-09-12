#pragma once

#include <limits>
#include <stddef.h>
#include <stdint.h>
#include "chimp_utils.hpp"
#include "bit_stream.hpp"
#include "ring_buffer.hpp"

//! The index is not incremented for the first value, is that a bug?
//! Ah nope, they just don't increment index in WriteFirst, and then proceed to index++ before setting the index in the other cases....

namespace duckdb_chimp {

struct Chimp128CompressionState {

	Chimp128CompressionState(double* output_stream, size_t stream_size) :
		output(output_stream, stream_size),
		ring_buffer(),
		previous_leading_zeroes(std::numeric_limits<int32_t>::max()) {}

	static int64_t Xor(Chimp128CompressionState& state, double val) {
		int64_t xor_result = state.stored_value ^ (uint64_t)val;
		state.stored_value = (int64_t)val;
		return xor_result;
	}

	void SetLeadingZeros(int32_t value = std::numeric_limits<int32_t>::max()) {
		this->previous_leading_zeroes = value;
	}

	BitStream<double>	output; //The stream to write to
	RingBuffer			ring_buffer; //! The ring buffer that holds the previous values
	int32_t				previous_leading_zeroes; //! The leading zeroes of the reference value
};

struct Chimp128Compression {

	//this.previousValuesLog2 =  (int)(Math.log(previousValues) / Math.log(2));
	//! With 'previous_values' set to 128 this resolves to 7
	static constexpr uint8_t PREVIOUS_VALUES_LOG2 = 7;
	static constexpr uint8_t FLAG_ZERO_SIZE = PREVIOUS_VALUES_LOG2 + 2;
	static constexpr uint8_t FLAG_ONE_SIZE = PREVIOUS_VALUES_LOG2 + 11;

	//this.threshold = 6 + previousValuesLog2;
	static constexpr uint8_t TRAILING_ZERO_THRESHOLD = 6 + PREVIOUS_VALUES_LOG2;

	template <bool FIRST>
	static void Store(double in, Chimp128CompressionState& state) {
		if (FIRST) {
			state.output.WriteValue<double, sizeof(double)>(in);
		}
		else {
			CompressValue(in, state);
		}
	}

	static void WriteFirst(double in, Chimp128CompressionState& state) {
		state.ring_buffer.Insert(in);
		state.output.WriteValue<double, sizeof(double)>(in);
	}

	//TODO: can this just have a templated FIRST bool?
	static void CompressValue(double in, Chimp128CompressionState& state) {
		auto key = state.ring_buffer.Key(in);
		uint64_t xor_result;
		uint32_t previous_index;
		uint32_t trailing_zeroes = 0;

		//! Initialize some relevant variables based on the state of the ring_buffer
		if (state.ring_buffer.Size() - key < RingBuffer::RING_SIZE) {
			auto current_index = state.ring_buffer.IndexOf(key);
			uint64_t tempxor_result = (uint64_t)in ^ current_index % RingBuffer::RING_SIZE;
			auto trailing_zeroes = __builtin_ctzll(tempxor_result);
			if (trailing_zeroes > TRAILING_ZERO_THRESHOLD) {
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
			//! The two values are identical
			//! FIXME: 'previous_index' is guaranteed to be only 8 bits, because it's less than 128
			//! Yet we use 9 bits to write it to the output stream
			state.output.WriteValue<uint32_t, FLAG_ZERO_SIZE>(previous_index);
			state.SetLeadingZeros();
		}
		else {
			//! Values are not identical
			uint64_t leading_zeroes = ChimpConstants::LEADING_ROUND[__builtin_clzll(xor_result)];

			if (trailing_zeroes > TRAILING_ZERO_THRESHOLD) {
				int32_t significant_bits = sizeof(double) - leading_zeroes - trailing_zeroes;
				auto result = 512 * (RingBuffer::RING_SIZE + previous_index) + 64 * ChimpConstants::LEADING_REPRESENTATION[leading_zeroes] + significant_bits;
				state.output.WriteValue<int32_t, FLAG_ONE_SIZE>(result);
				state.output.WriteValue<int64_t>(xor_result >> trailing_zeroes, significant_bits);
				state.SetLeadingZeros();
			}
			else if (leading_zeroes == state.previous_leading_zeroes) {
				int32_t significant_bits = sizeof(double) - leading_zeroes;
				state.output.WriteValue<uint8_t, 2>(2);
				state.output.WriteValue<uint64_t>(xor_result, significant_bits);
			}
			else {
				int32_t significant_bits = sizeof(double) - leading_zeroes;
				state.output.WriteValue<uint32_t, 5>(24 + ChimpConstants::LEADING_REPRESENTATION[leading_zeroes]);
				state.output.WriteValue<uint64_t>(xor_result, significant_bits);
				state.SetLeadingZeros(leading_zeroes);
			}
		}
		state.ring_buffer.Insert(in);
	}
};

} //namespace duckdb_chimp
