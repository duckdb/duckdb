#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/types/bit.hpp"
#include "duckdb/common/types/string_type.hpp"

namespace duckdb {

void Bit::SetEmptyBitString(string_t &target, string_t &input) {
	char *res_buf = target.GetDataWriteable();
	const char *buf = input.GetDataUnsafe();
	memset(res_buf, 0, input.GetSize());
	res_buf[0] = buf[0];
}

idx_t Bit::BitLength(string_t bits) {
	return ((bits.GetSize() - 1) * 8) - GetPadding(bits);
}

idx_t Bit::OctetLength(string_t bits) {
	return bits.GetSize() - 1;
}

idx_t Bit::BitCount(string_t bits) {
	idx_t count = 0;
	const char *buf = bits.GetDataUnsafe();
	for (idx_t byte_idx = 1; byte_idx < OctetLength(bits) + 1; byte_idx++) {
		for (idx_t bit_idx = 0; bit_idx < 8; bit_idx++) {
			count += (buf[byte_idx] & (1 << bit_idx)) ? 1 : 0;
		}
	}
	return count;
}

idx_t Bit::BitPosition(string_t substring, string_t bits) {
	const char *buf = bits.GetDataUnsafe();
	auto len = bits.GetSize();
	auto substr_len = BitLength(substring);
	idx_t substr_idx = 0;

	for (idx_t bit_idx = GetPadding(bits); bit_idx < 8; bit_idx++) {
		idx_t bit = buf[1] & (1 << (7 - bit_idx)) ? 1 : 0;
		if (bit == GetBit(substring, substr_idx)) {
			substr_idx++;
			if (substr_idx == substr_len) {
				return (bit_idx - GetPadding(bits)) - substr_len + 2;
			}
		} else {
			substr_idx = 0;
		}
	}

	for (idx_t byte_idx = 2; byte_idx < len; byte_idx++) {
		for (idx_t bit_idx = 0; bit_idx < 8; bit_idx++) {
			idx_t bit = buf[byte_idx] & (1 << (7 - bit_idx)) ? 1 : 0;
			if (bit == GetBit(substring, substr_idx)) {
				substr_idx++;
				if (substr_idx == substr_len) {
					return (((byte_idx - 1) * 8) + bit_idx - GetPadding(bits)) - substr_len + 2;
				}
			} else {
				substr_idx = 0;
			}
		}
	}
	return 0;
}

void Bit::ToString(string_t bits, char *output) {
	auto data = (const_data_ptr_t)bits.GetDataUnsafe();
	auto len = bits.GetSize();

	idx_t padding = GetPadding(bits);
	idx_t output_idx = 0;
	for (idx_t bit_idx = padding; bit_idx < 8; bit_idx++) {
		output[output_idx++] = data[1] & (1 << (7 - bit_idx)) ? '1' : '0';
	}
	for (idx_t byte_idx = 2; byte_idx < len; byte_idx++) {
		for (idx_t bit_idx = 0; bit_idx < 8; bit_idx++) {
			output[output_idx++] = data[byte_idx] & (1 << (7 - bit_idx)) ? '1' : '0';
		}
	}
}

string Bit::ToString(string_t str) {
	auto len = BitLength(str);
	auto buffer = std::unique_ptr<char[]>(new char[len]);
	ToString(str, buffer.get());
	return string(buffer.get(), len);
}

bool Bit::TryGetBitStringSize(string_t str, idx_t &str_len, string *error_message) {
	auto data = (const_data_ptr_t)str.GetDataUnsafe();
	auto len = str.GetSize();
	str_len = 0;
	for (idx_t i = 0; i < len; i++) {
		if (data[i] == '0' || data[i] == '1') {
			str_len++;
		} else {
			string error = StringUtil::Format("Invalid character encountered in string -> bit conversion: '%s'",
			                                  string((char *)data + i, 1));
			HandleCastError::AssignError(error, error_message);
			return false;
		}
	}
	str_len = str_len % 8 ? (str_len / 8) + 1 : str_len / 8;
	str_len++; // additional first byte to store info on zero padding
	return true;
}

idx_t Bit::GetBitSize(string_t str) {
	string error_message;
	idx_t str_len;
	if (!Bit::TryGetBitStringSize(str, str_len, &error_message)) {
		throw ConversionException(error_message);
	}
	return str_len;
}

void Bit::ToBit(string_t str, data_ptr_t output) {
	auto data = (const_data_ptr_t)str.GetDataUnsafe();
	auto len = str.GetSize();

	char byte = 0;
	idx_t padded_byte = len % 8;
	for (idx_t i = 0; i < padded_byte; i++) {
		byte <<= 1;
		if (data[i] == '1') {
			byte |= 1;
		}
	}
	if (padded_byte != 0) {
		*(output++) = (8 - padded_byte); // the first byte contains the number of padded zeroes
	}
	*(output++) = byte;

	for (idx_t byte_idx = padded_byte; byte_idx < len; byte_idx += 8) {
		byte = 0;
		for (idx_t bit_idx = 0; bit_idx < 8; bit_idx++) {
			byte <<= 1;
			if (data[byte_idx + bit_idx] == '1') {
				byte |= 1;
			}
		}
		*(output++) = byte;
	}
}

string Bit::ToBit(string_t str) {
	auto bit_len = GetBitSize(str);
	auto buffer = std::unique_ptr<char[]>(new char[bit_len]);
	Bit::ToBit(str, (data_ptr_t)buffer.get());
	return string(buffer.get(), bit_len);
}

idx_t Bit::GetBit(string_t bit_string, idx_t n) {
	const char *buf = bit_string.GetDataUnsafe();
	n += GetPadding(bit_string);

	char byte = buf[(n / 8) + 1] >> (7 - (n % 8));
	return (byte & 1 ? 1 : 0);
}

void Bit::SetBit(const string_t &bit_string, idx_t n, idx_t new_value, string_t &result) {
	char *result_buf = result.GetDataWriteable();
	const char *buf = bit_string.GetDataUnsafe();
	n += GetPadding(bit_string);

	memcpy(result_buf, buf, bit_string.GetSize());
	char shift_byte = 1 << (7 - (n % 8));
	if (new_value == 0) {
		shift_byte = ~shift_byte;
		result_buf[(n / 8) + 1] = buf[(n / 8) + 1] & shift_byte;
	} else {
		result_buf[(n / 8) + 1] = buf[(n / 8) + 1] | shift_byte;
	}
}

void Bit::SetBit(string_t &bit_string, idx_t n, idx_t new_value) {
	char *buf = bit_string.GetDataWriteable();
	n += GetPadding(bit_string);

	char shift_byte = 1 << (7 - (n % 8));
	if (new_value == 0) {
		shift_byte = ~shift_byte;
		buf[(n / 8) + 1] &= shift_byte;
	} else {
		buf[(n / 8) + 1] |= shift_byte;
	}
}

inline idx_t Bit::GetPadding(const string_t &bit_string) {
	auto data = (const_data_ptr_t)bit_string.GetDataUnsafe();
	return data[0];
}

// **** BITWISE OPERATORS ****
void Bit::RightShift(const string_t &bit_string, const idx_t &shift, string_t &result) {
	char *res_buf = result.GetDataWriteable();
	const char *buf = bit_string.GetDataUnsafe();
	res_buf[0] = buf[0];
	for (idx_t i = 0; i < Bit::BitLength(result); i++) {
		if (i < shift) {
			Bit::SetBit(result, i, 0);
		} else {
			idx_t bit = Bit::GetBit(bit_string, i - shift);
			Bit::SetBit(result, i, bit);
		}
	}
}

void Bit::LeftShift(const string_t &bit_string, const idx_t &shift, string_t &result) {
	char *res_buf = result.GetDataWriteable();
	const char *buf = bit_string.GetDataUnsafe();
	res_buf[0] = buf[0];
	for (idx_t i = 0; i < Bit::BitLength(bit_string); i++) {
		if (i < (Bit::BitLength(bit_string) - shift)) {
			idx_t bit = Bit::GetBit(bit_string, shift + i);
			Bit::SetBit(result, i, bit);
		} else {
			Bit::SetBit(result, i, 0);
		}
	}
}

void Bit::BitwiseAnd(const string_t &rhs, const string_t &lhs, string_t &result) {
	if (Bit::BitLength(lhs) != Bit::BitLength(rhs)) {
		throw InvalidInputException("Cannot AND bit strings of different sizes");
	}

	char *buf = result.GetDataWriteable();
	const char *r_buf = rhs.GetDataUnsafe();
	const char *l_buf = lhs.GetDataUnsafe();

	buf[0] = l_buf[0];
	for (idx_t i = 1; i < lhs.GetSize(); i++) {
		buf[i] = l_buf[i] & r_buf[i];
	}
}

void Bit::BitwiseOr(const string_t &rhs, const string_t &lhs, string_t &result) {
	if (Bit::BitLength(lhs) != Bit::BitLength(rhs)) {
		throw InvalidInputException("Cannot OR bit strings of different sizes");
	}

	char *buf = result.GetDataWriteable();
	const char *r_buf = rhs.GetDataUnsafe();
	const char *l_buf = lhs.GetDataUnsafe();

	buf[0] = l_buf[0];
	for (idx_t i = 1; i < lhs.GetSize(); i++) {
		buf[i] = l_buf[i] | r_buf[i];
	}
}

void Bit::BitwiseXor(const string_t &rhs, const string_t &lhs, string_t &result) {
	if (Bit::BitLength(lhs) != Bit::BitLength(rhs)) {
		throw InvalidInputException("Cannot XOR bit strings of different sizes");
	}

	char *buf = result.GetDataWriteable();
	const char *r_buf = rhs.GetDataUnsafe();
	const char *l_buf = lhs.GetDataUnsafe();

	buf[0] = l_buf[0];
	for (idx_t i = 1; i < lhs.GetSize(); i++) {
		buf[i] = l_buf[i] ^ r_buf[i];
	}
}

void Bit::BitwiseNot(const string_t &input, string_t &result) {
	char *result_buf = result.GetDataWriteable();
	const char *buf = input.GetDataUnsafe();

	result_buf[0] = buf[0];
	for (idx_t i = 1; i < input.GetSize(); i++) {
		result_buf[i] = ~buf[i];
	}
}
} // namespace duckdb
