#include "duckdb/common/types/varint.hpp"
#include <cmath>

namespace duckdb {

void Varint::Verify(const string_t &input) {
#ifdef DEBUG
	// Size must be >= 4
	idx_t varint_bytes = input.GetSize();
	if (varint_bytes < 4) {
		throw InternalException("Varint number of bytes is invalid, current number of bytes is %d", varint_bytes);
	}
	// Bytes in header must quantify the number of data bytes
	auto varint_ptr = input.GetData();
	bool is_negative = (varint_ptr[0] & 0x80) == 0;
	uint32_t number_of_bytes = 0;
	char mask = 0x7F;
	if (is_negative) {
		number_of_bytes |= static_cast<uint32_t>(~varint_ptr[0] & mask) << 16 & 0xFF0000;
		number_of_bytes |= static_cast<uint32_t>(~varint_ptr[1]) << 8 & 0xFF00;
		;
		number_of_bytes |= static_cast<uint32_t>(~varint_ptr[2]) & 0xFF;
	} else {
		number_of_bytes |= static_cast<uint32_t>(varint_ptr[0] & mask) << 16 & 0xFF0000;
		number_of_bytes |= static_cast<uint32_t>(varint_ptr[1]) << 8 & 0xFF00;
		number_of_bytes |= static_cast<uint32_t>(varint_ptr[2]) & 0xFF;
	}
	if (number_of_bytes != varint_bytes - 3) {
		throw InternalException("The number of bytes set in the Varint header: %d bytes. Does not "
		                        "match the number of bytes encountered as the varint data: %d bytes.",
		                        number_of_bytes, varint_bytes - 3);
	}
	//  No bytes between 4 and end can be 0, unless total size == 4
	if (varint_bytes > 4) {
		if (is_negative) {
			if (~varint_ptr[3] == 0) {
				throw InternalException("Invalid top data bytes set to 0 for VARINT values");
			}
		} else {
			if (varint_ptr[3] == 0) {
				throw InternalException("Invalid top data bytes set to 0 for VARINT values");
			}
		}
	}
#endif
}
void Varint::SetHeader(char *blob, uint64_t number_of_bytes, bool is_negative) {
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
string_t Varint::InitializeVarintZero(Vector &result) {
	uint32_t blob_size = 1 + VARINT_HEADER_SIZE;
	auto blob = StringVector::EmptyString(result, blob_size);
	auto writable_blob = blob.GetDataWriteable();
	SetHeader(writable_blob, 1, false);
	writable_blob[3] = 0;
	blob.Finalize();
	return blob;
}

int Varint::CharToDigit(char c) {
	return c - '0';
}

char Varint::DigitToChar(int digit) {
	return static_cast<char>(digit) + '0';
}

bool Varint::VarcharFormatting(string_t &value, idx_t &start_pos, idx_t &end_pos, bool &is_negative, bool &is_zero) {
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
		// This is either a '+' or '-'. Hence invalid.
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

void Varint::GetByteArray(vector<uint8_t> &byte_array, bool &is_negative, const string_t &blob) {
	if (blob.GetSize() < 4) {
		throw InvalidInputException("Invalid blob size.");
	}
	auto blob_ptr = blob.GetData();

	// Determine if the number is negative
	is_negative = (blob_ptr[0] & 0x80) == 0;
	for (idx_t i = 3; i < blob.GetSize(); i++) {
		if (is_negative) {
			byte_array.push_back(static_cast<uint8_t>(~blob_ptr[i]));
		} else {
			byte_array.push_back(static_cast<uint8_t>(blob_ptr[i]));
		}
	}
}

string Varint::VarIntToVarchar(const string_t &blob) {
	string decimal_string;
	vector<uint8_t> byte_array;
	bool is_negative;
	GetByteArray(byte_array, is_negative, blob);
	while (!byte_array.empty()) {
		string quotient;
		uint8_t remainder = 0;
		for (uint8_t byte : byte_array) {
			int new_value = remainder * 256 + byte;
			quotient += DigitToChar(new_value / 10);
			remainder = static_cast<uint8_t>(new_value % 10);
		}
		decimal_string += DigitToChar(remainder);
		// Remove leading zeros from the quotient
		byte_array.clear();
		for (char digit : quotient) {
			if (digit != '0' || !byte_array.empty()) {
				byte_array.push_back(static_cast<uint8_t>(CharToDigit(digit)));
			}
		}
	}
	if (is_negative) {
		decimal_string += '-';
	}
	// Reverse the string to get the correct decimal representation
	std::reverse(decimal_string.begin(), decimal_string.end());
	return decimal_string;
}

bool Varint::VarintToDouble(string_t &blob, double &result, bool &strict) {
	result = 0;
	bool is_negative;

	if (blob.GetSize() < 4) {
		throw InvalidInputException("Invalid blob size.");
	}
	auto blob_ptr = blob.GetData();

	// Determine if the number is negative
	is_negative = (blob_ptr[0] & 0x80) == 0;
	idx_t byte_pos = 0;
	for (idx_t i = blob.GetSize() - 1; i > 2; i--) {
		if (is_negative) {
			result += static_cast<uint8_t>(~blob_ptr[i]) * pow(256, byte_pos);
		} else {
			result += static_cast<uint8_t>(blob_ptr[i]) * pow(256, byte_pos);
		}
		byte_pos++;
	}

	if (is_negative) {
		result *= -1;
	}
	return std::isfinite(result);
}

} // namespace duckdb
