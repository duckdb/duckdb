#pragma once

#include <limits>
#include <stddef.h>

namespace duckdb_chimp {

//! Small wrapper around a double + uint8_t to write bits into a double and keep track of the amount of bits written
struct BitArray {
	static constexpr BITS = sizeof(double) * 8;

	BitArray(double& val, uint8_t& compressed_size) : val(val), bit_idx(compressed_size) {}

	template <bool BIT>
	void WriteBit() {}

	template <>
	void WriteBit<true>() {
		val |= ((uint64_t)1 << (BITS - 1 - bit_idx++));
	}
	void WriteBit<false>() {
		bit_idx++;
	}

	template <class T, uint8_t VALUE_SIZE>
	void WriteValue(T value) {
		for (uint8_t i = 0; i < VALUE_SIZE; i++) {
			if (((value >> i) << (BITS - 1 - i)) & 1) {
				WriteBit<true>();
			}
			else {
				WriteBit<false>();
			}
		}
	}
	template <class T>
	void WriteValue(T value, uint8_t value_size) {
		for (uint8_t i = 0; i < value_size; i++) {
			if (((value >> i) << (BITS - 1 - i)) & 1) {
				WriteBit<true>();
			}
			else {
				WriteBit<false>();
			}
		}
	}

	double& val;
	uint8_t& bit_idx;
}

struct ChimpCompressionState {
	ChimpCompressionState() :
		previous_leading_zeroes(std::numeric_limits<int32_t>::max()),
		stored_value(0),
		first(true),
	{}

	static int64_t Xor(ChimpCompressionState& state, double val) {
		int64_t xor = state.stored_value ^ val;
		state.stored_value = (int64_t)val;
		return xor;
	}
	static void ResetStoredLeadingZeroes(ChimpCompressionState& state) {
		state.previous_leading_zeroes = std::numeric_limits<int32_t>::max();
	}

	int32_t previous_leading_zeroes;
	int64_t stored_value;
	bool first;
	int32_t size;
};

static const uint8_t LEADING_REPRESENTATION[] = {
	0, 0, 0, 0, 0, 0, 0, 0,
	1, 1, 1, 1, 2, 2, 2, 2,
	3, 3, 4, 4, 5, 5, 6, 6,
	7, 7, 7, 7, 7, 7, 7, 7,
	7, 7, 7, 7, 7, 7, 7, 7,
	7, 7, 7, 7, 7, 7, 7, 7,
	7, 7, 7, 7, 7, 7, 7, 7,
	7, 7, 7, 7, 7, 7, 7, 7
};

static const uint8_t LEADING_ROUND[] = {
	0,  0,  0,  0,  0,  0,  0,  0,
	8,  8,  8,  8,  12, 12, 12, 12,
	16, 16, 18, 18, 20, 20, 22, 22,
	24, 24, 24, 24, 24, 24, 24, 24,
	24, 24, 24, 24, 24, 24, 24, 24,
	24, 24, 24, 24, 24, 24, 24, 24,
	24, 24, 24, 24, 24, 24, 24, 24,
	24, 24, 24, 24, 24, 24, 24, 24
};

__builtin_clzll(v)

struct ChimpCompression {
	static constexpr TRAILING_ZERO_THRESHOLD = 6;

	uint8_t LeadingZeroes(uint64_t x)
	{
		static const char table[32] = {
			0, 31, 9, 30, 3, 8, 13, 29, 2, 5, 7, 21, 12, 24, 28, 19,
			1, 10, 4, 14, 6, 22, 25, 20, 11, 15, 23, 26, 16, 27, 17, 18
		};
		x |= x>>1;
		x |= x>>2;
		x |= x>>4;
		x |= x>>8;
		x |= x>>16;
		x++;
		return table[x*0x076be629>>27];
	}

	//TODO: can this just have a templated FIRST bool?
	static void CompressValue(double in, double& out, uint8_t& compressed_size, ChimpCompressionState& state) {
		BitArray value(out, compressed_size);
		int64_t xor = ChimpCompressionState::Xor(state, in);
		if (!xor) {
			value.WriteBit<false>();
			value.WriteBit<false>();
		}
		else {
			auto leading_zeroes = __builtin_clzll(xor);
			leading_zeries = LEADING_ROUND[leading_zeroes];
			auto trailing_zeroes = __builtin_ctzll(xor);

			if (trailing_zeroes > ChimpCompression::TRAILING_ZERO_THRESHOLD) {
				value.WriteBit<false>();
				value.WriteBit<true>();
				uint8_t significant_bits = sizeof(double) - leading_zeroes - trailing_zeroes;
				value.WriteValue<uint8_t, 3>(LEADING_REPRESENTATION[leading_zeroes]);
				value.WriteValue<uint8_t, 6>(significant_bits);
				value.WriteValue((uint64_t)xor >> trailing_zeroes, significant_bits);
				ChimpCompressionState::ResetStoredLeadingZeroes(state);
			}
			else if (leading_zeroes == state.previous_leading_zeroes) {
				value.WriteBit<true>();
				value.WriteBit<false>();
				int8_t significant_bits = sizeof(double) - leading_zeroes;
				value.WriteValue(xor, significant_bits);
			}
			else {
				value.WriteBit<true>();
				value.WriteBit<true>();
				uint8_t significant_bits = sizeof(double) - leading_zeroes;
				state.previous_leading_zeroes = leading_zeroes;
				value.WriteValue<uint8_t, 3>(LEADING_REPRESENTATION[leading_zeroes]);
				value.WriteValue(xor, significant_bits);
			}
		}
	}
}

} //namespace duckdb_chimp
