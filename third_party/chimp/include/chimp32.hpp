#pragma once

#include <limits>
#include <stddef.h>
#include <stdint.h>
#include "chimp_utils.hpp"
#include "output_bit_stream.hpp"

namespace duckdb_chimp {

struct Chimp32CompressionState {
	Chimp32CompressionState(double* output_stream, size_t stream_size) :
		output(output_stream, stream_size),
		previous_leading_zeroes(std::numeric_limits<int32_t>::max()),
		stored_value(0),
		first(true)
	{}

	static int64_t Xor(Chimp32CompressionState& state, double val) {
		int64_t xor_result = state.stored_value ^ (uint64_t)val;
		state.stored_value = (int64_t)val;
		return xor_result;
	}
	static void ResetStoredLeadingZeroes(Chimp32CompressionState& state) {
		state.previous_leading_zeroes = std::numeric_limits<int32_t>::max();
	}

	OutputBitStream<double> output;
	int32_t previous_leading_zeroes;
	int64_t stored_value;
	bool first;
	int32_t size;
};

struct Chimp32Compression {
	static constexpr uint8_t TRAILING_ZERO_THRESHOLD = 6;

	template <bool FIRST>
	static void Store(double in, Chimp32CompressionState& state) {
		if (FIRST) {
			state.output.WriteValue<double, sizeof(double)>(in);
		}
		else {
			CompressValue(in, state);
		}
	}

	//TODO: can this just have a templated FIRST bool?
	static void CompressValue(double in, double& out, uint8_t& compressed_size, Chimp32CompressionState& state) {
		int64_t xor_result = Chimp32CompressionState::Xor(state, in);
		if (!xor_result) {
			state.output.WriteBit(false);
			state.output.WriteBit(false);
		}
		else {
			auto leading_zeroes = __builtin_clzll(xor_result);
			leading_zeroes = ChimpConstants::LEADING_ROUND[leading_zeroes];
			auto trailing_zeroes = __builtin_ctzll(xor_result);

			if (trailing_zeroes > Chimp32Compression::TRAILING_ZERO_THRESHOLD) {
				state.output.WriteBit(false);
				state.output.WriteBit(true);
				uint8_t significant_bits = sizeof(double) - leading_zeroes - trailing_zeroes;
				state.output.WriteValue<uint8_t>(ChimpConstants::LEADING_REPRESENTATION[leading_zeroes], 3);
				state.output.WriteValue<uint8_t>(significant_bits, 6);
				state.output.WriteValue<uint64_t>((uint64_t)xor_result >> trailing_zeroes, significant_bits);
				Chimp32CompressionState::ResetStoredLeadingZeroes(state);
			}
			else if (leading_zeroes == state.previous_leading_zeroes) {
				state.output.WriteBit(true);
				state.output.WriteBit(false);
				int8_t significant_bits = sizeof(double) - leading_zeroes;
				state.output.WriteValue<uint64_t>(xor_result, significant_bits);
			}
			else {
				state.output.WriteBit(true);
				state.output.WriteBit(true);
				uint8_t significant_bits = sizeof(double) - leading_zeroes;
				state.previous_leading_zeroes = leading_zeroes;
				state.output.WriteValue<uint8_t>(ChimpConstants::LEADING_REPRESENTATION[leading_zeroes], 3);
				state.output.WriteValue(xor_result, significant_bits);
			}
		}
	}
};

} //namespace duckdb_chimp
