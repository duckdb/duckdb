#include "duckdb/common/bignum.hpp"
#include "duckdb/common/types/bignum.hpp"
#include "duckdb/common/exception/conversion_exception.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/typedefs.hpp"
#include <cmath>

namespace duckdb {

void Bignum::Verify(const bignum_t &input) {
#ifdef DEBUG
	// Size must be >= 4
	idx_t bignum_bytes = input.data.GetSize();
	if (bignum_bytes < 4) {
		throw InternalException("Bignum number of bytes is invalid, current number of bytes is %d", bignum_bytes);
	}
	// Bytes in header must quantify the number of data bytes
	auto bignum_ptr = input.data.GetData();
	bool is_negative = (bignum_ptr[0] & 0x80) == 0;
	uint32_t number_of_bytes = 0;
	if (bignum_bytes == 4 && is_negative) {
		// There is only one invalid value, which is -0
		if (bignum_ptr[3] == static_cast<char>(0xFF)) {
			throw InternalException("Bignum value -0 is not allowed in the Bignum specification.");
		}
	}

	char mask = 0x7F;
	if (is_negative) {
		number_of_bytes |= static_cast<uint32_t>(~bignum_ptr[0] & mask) << 16 & 0xFF0000;
		number_of_bytes |= static_cast<uint32_t>(~bignum_ptr[1]) << 8 & 0xFF00;
		;
		number_of_bytes |= static_cast<uint32_t>(~bignum_ptr[2]) & 0xFF;
	} else {
		number_of_bytes |= static_cast<uint32_t>(bignum_ptr[0] & mask) << 16 & 0xFF0000;
		number_of_bytes |= static_cast<uint32_t>(bignum_ptr[1]) << 8 & 0xFF00;
		number_of_bytes |= static_cast<uint32_t>(bignum_ptr[2]) & 0xFF;
	}
	if (number_of_bytes != bignum_bytes - 3) {
		throw InternalException("The number of bytes set in the Bignum header: %d bytes. Does not "
		                        "match the number of bytes encountered as the bignum data: %d bytes.",
		                        number_of_bytes, bignum_bytes - 3);
	}
	//  No bytes between 4 and end can be 0, unless total size == 4
	if (bignum_bytes > 4) {
		if (is_negative) {
			if (static_cast<data_t>(~bignum_ptr[3]) == 0) {
				throw InternalException("Invalid top data bytes set to 0 for BIGNUM values");
			}
		} else {
			if (bignum_ptr[3] == 0) {
				throw InternalException("Invalid top data bytes set to 0 for BIGNUM values");
			}
		}
	}
#endif
}
void Bignum::SetHeader(char *blob, uint64_t number_of_bytes, bool is_negative) {
	uint32_t header = static_cast<uint32_t>(number_of_bytes);
	// Set MSBit of 3rd byte
	header |= 0x00800000;
	if (is_negative) {
		header = ~header;
	}
	// we ignore MSByte  of header.
	// write the 3 bytes to blob.
	blob[0] = static_cast<char>(header >> 16);
	blob[1] = static_cast<char>(header >> 8 & 0xFF);
	blob[2] = static_cast<char>(header & 0xFF);
}

// Creates a blob representing the value 0
bignum_t Bignum::InitializeBignumZero(Vector &result) {
	uint32_t blob_size = 1 + BIGNUM_HEADER_SIZE;
	auto blob = StringVector::EmptyString(result, blob_size);
	auto writable_blob = blob.GetDataWriteable();
	SetHeader(writable_blob, 1, false);
	writable_blob[3] = 0;
	blob.Finalize();
	const bignum_t result_bignum(blob);
	return result_bignum;
}

string Bignum::InitializeBignumZero() {
	uint32_t blob_size = 1 + BIGNUM_HEADER_SIZE;
	string result(blob_size, '0');
	SetHeader(&result[0], 1, false);
	result[3] = 0;
	return result;
}

int Bignum::CharToDigit(char c) {
	return c - '0';
}

char Bignum::DigitToChar(int digit) {
	// FIXME: this would be the proper solution:
	// return UnsafeNumericCast<char>(digit + '0');
	return static_cast<char>(digit + '0');
}

bool Bignum::VarcharFormatting(const string_t &value, idx_t &start_pos, idx_t &end_pos, bool &is_negative,
                               bool &is_zero) {
	// If it's empty we error
	if (value.Empty()) {
		return false;
	}
	start_pos = 0;
	is_zero = false;

	auto int_value_char = value.GetData();
	end_pos = value.GetSize();

	// If first character is -, we have a negative number, if + we have a + number
	is_negative = int_value_char[0] == '-';
	if (is_negative) {
		start_pos++;
	}
	if (int_value_char[0] == '+') {
		start_pos++;
	}
	// Now lets trim 0s
	bool at_least_one_zero = false;
	while (start_pos < end_pos && int_value_char[start_pos] == '0') {
		start_pos++;
		at_least_one_zero = true;
	}
	if (start_pos == end_pos) {
		if (at_least_one_zero) {
			// This is a 0 value
			is_zero = true;
			return true;
		}
		// This is either a '+' or '-'. Hence, invalid.
		return false;
	}
	idx_t cur_pos = start_pos;
	// Verify all is numeric
	while (cur_pos < end_pos && std::isdigit(int_value_char[cur_pos])) {
		cur_pos++;
	}
	if (cur_pos < end_pos) {
		idx_t possible_end = cur_pos;
		// Oh oh, this is not a digit, if it's a . we might be fine, otherwise, this is invalid.
		if (int_value_char[cur_pos] == '.') {
			cur_pos++;
		} else {
			return false;
		}

		while (cur_pos < end_pos) {
			if (std::isdigit(int_value_char[cur_pos])) {
				cur_pos++;
			} else {
				// By now we can only have numbers, otherwise this is invalid.
				return false;
			}
		}
		// Floor cast this boy
		end_pos = possible_end;
	}
	return true;
}

void Bignum::GetByteArray(vector<uint8_t> &byte_array, bool &is_negative, const string_t &blob) {
	if (blob.GetSize() < 4) {
		throw InvalidInputException("Invalid blob size.");
	}
	auto blob_ptr = blob.GetData();

	// Determine if the number is negative
	is_negative = (blob_ptr[0] & 0x80) == 0;
	byte_array.reserve(blob.GetSize() - 3);
	if (is_negative) {
		for (idx_t i = 3; i < blob.GetSize(); i++) {
			byte_array.push_back(static_cast<uint8_t>(~blob_ptr[i]));
		}
	} else {
		for (idx_t i = 3; i < blob.GetSize(); i++) {
			byte_array.push_back(static_cast<uint8_t>(blob_ptr[i]));
		}
	}
}

string Bignum::FromByteArray(uint8_t *data, idx_t size, bool is_negative) {
	string result(BIGNUM_HEADER_SIZE + size, '0');
	SetHeader(&result[0], size, is_negative);
	uint8_t *result_data = reinterpret_cast<uint8_t *>(&result[BIGNUM_HEADER_SIZE]);
	if (is_negative) {
		for (idx_t i = 0; i < size; i++) {
			result_data[i] = ~data[i];
		}
	} else {
		for (idx_t i = 0; i < size; i++) {
			result_data[i] = data[i];
		}
	}
	return result;
}

// Following CPython and Knuth (TAOCP, Volume 2 (3rd edn), section 4.4, Method 1b).
string Bignum::BignumToVarchar(const bignum_t &blob) {
	string decimal_string;
	vector<uint8_t> byte_array;
	bool is_negative;
	GetByteArray(byte_array, is_negative, blob.data);
	vector<digit_t> digits;
	// Rounding byte_array to digit_bytes multiple size, so that we can process every digit_bytes bytes
	// at a time without if check in the for loop
	idx_t padding_size = (-byte_array.size()) & (DIGIT_BYTES - 1);
	byte_array.insert(byte_array.begin(), padding_size, 0);
	for (idx_t i = 0; i < byte_array.size(); i += DIGIT_BYTES) {
		digit_t hi = 0;
		for (idx_t j = 0; j < DIGIT_BYTES; j++) {
			hi |= UnsafeNumericCast<digit_t>(byte_array[i + j]) << (8 * (DIGIT_BYTES - j - 1));
		}

		for (idx_t j = 0; j < digits.size(); j++) {
			twodigit_t tmp = UnsafeNumericCast<twodigit_t>(digits[j]) << DIGIT_BITS | hi;
			hi = static_cast<digit_t>(tmp / UnsafeNumericCast<twodigit_t>(DECIMAL_BASE));
			digits[j] = static_cast<digit_t>(tmp - UnsafeNumericCast<twodigit_t>(DECIMAL_BASE * hi));
		}

		while (hi) {
			digits.push_back(hi % DECIMAL_BASE);
			hi /= DECIMAL_BASE;
		}
	}

	if (digits.empty()) {
		digits.push_back(0);
	}

	for (idx_t i = 0; i < digits.size() - 1; i++) {
		auto remain = digits[i];
		for (idx_t j = 0; j < DECIMAL_SHIFT; j++) {
			decimal_string += DigitToChar(static_cast<int>(remain % 10));
			remain /= 10;
		}
	}

	auto remain = digits.back();
	do {
		decimal_string += DigitToChar(static_cast<int>(remain % 10));
		remain /= 10;
	} while (remain != 0);

	if (is_negative) {
		decimal_string += '-';
	}
	// Reverse the string to get the correct decimal representation
	std::reverse(decimal_string.begin(), decimal_string.end());
	return decimal_string;
}

string Bignum::VarcharToBignum(const string_t &value) {
	idx_t start_pos, end_pos;
	bool is_negative, is_zero;
	if (!VarcharFormatting(value, start_pos, end_pos, is_negative, is_zero)) {
		throw ConversionException("Could not convert string \'%s\' to Bignum", value.GetString());
	}
	if (is_zero) {
		// Return Value 0
		return InitializeBignumZero();
	}
	auto int_value_char = value.GetData();
	idx_t actual_size = end_pos - start_pos;

	// we initalize result with space for our header
	string result(BIGNUM_HEADER_SIZE, '0');
	unsafe_vector<uint64_t> digits;

	// The max number a uint64_t can represent is 18.446.744.073.709.551.615
	// That has 20 digits
	// In the worst case a remainder of a division will be 255, which is 3 digits
	// Since the max value is 184, we need to take one more digit out
	// Hence we end up with a max of 16 digits supported.
	constexpr uint8_t max_digits = 16;
	const idx_t number_of_digits = static_cast<idx_t>(std::ceil(static_cast<double>(actual_size) / max_digits));

	// lets convert the string to a uint64_t vector
	idx_t cur_end = end_pos;
	for (idx_t i = 0; i < number_of_digits; i++) {
		idx_t cur_start = static_cast<int64_t>(start_pos) > static_cast<int64_t>(cur_end - max_digits)
		                      ? start_pos
		                      : cur_end - max_digits;
		std::string current_number(int_value_char + cur_start, cur_end - cur_start);
		digits.push_back(std::stoull(current_number));
		// move cur_end to more digits down the road
		cur_end = cur_end - max_digits;
	}

	// Now that we have our uint64_t vector, lets start our division process to figure out the new number and remainder
	while (!digits.empty()) {
		idx_t digit_idx = digits.size() - 1;
		uint8_t remainder = 0;
		idx_t digits_size = digits.size();
		for (idx_t i = 0; i < digits_size; i++) {
			digits[digit_idx] += static_cast<uint64_t>(remainder * pow(10, max_digits));
			remainder = static_cast<uint8_t>(digits[digit_idx] % 256);
			digits[digit_idx] /= 256;
			if (digits[digit_idx] == 0 && digit_idx == digits.size() - 1) {
				// we can cap this
				digits.pop_back();
			}
			digit_idx--;
		}
		if (is_negative) {
			result.push_back(static_cast<char>(~remainder));
		} else {
			result.push_back(static_cast<char>(remainder));
		}
	}
	std::reverse(result.begin() + BIGNUM_HEADER_SIZE, result.end());
	// Set header after we know the size of the bignum
	SetHeader(&result[0], result.size() - BIGNUM_HEADER_SIZE, is_negative);
	return result;
}

bool Bignum::BignumToDouble(const bignum_t &blob, double &result, bool &strict) {
	result = 0;

	if (blob.data.GetSize() < 4) {
		throw InvalidInputException("Invalid blob size.");
	}
	auto blob_ptr = blob.data.GetData();

	// Determine if the number is negative
	bool is_negative = (blob_ptr[0] & 0x80) == 0;
	idx_t byte_pos = 0;
	for (idx_t i = blob.data.GetSize() - 1; i > 2; i--) {
		if (is_negative) {
			result += static_cast<uint8_t>(~blob_ptr[i]) * pow(256, static_cast<double>(byte_pos));
		} else {
			result += static_cast<uint8_t>(blob_ptr[i]) * pow(256, static_cast<double>(byte_pos));
		}
		byte_pos++;
	}

	if (is_negative) {
		result *= -1;
	}
	if (!std::isfinite(result)) {
		// We throw an error
		throw ConversionException("Could not convert bignum '%s' to Double", BignumToVarchar(blob));
	}
	return true;
}

} // namespace duckdb
