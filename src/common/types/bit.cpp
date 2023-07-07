#include "duckdb/common/assert.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/types/bit.hpp"
#include "duckdb/common/types/string_type.hpp"

namespace duckdb {

// **** helper functions ****
static char ComputePadding(idx_t len) {
	return (8 - (len % 8)) % 8;
}

idx_t Bit::ComputeBitstringLen(idx_t len) {
	idx_t result = len / 8;
	if (len % 8 != 0) {
		result++;
	}
	// additional first byte to store info on zero padding
	result++;
	return result;
}

static inline idx_t GetBitPadding(const string_t &bit_string) {
	auto data = const_data_ptr_cast(bit_string.GetData());
	D_ASSERT(idx_t(data[0]) <= 8);
	return data[0];
}

static inline idx_t GetBitSize(const string_t &str) {
	string error_message;
	idx_t str_len;
	if (!Bit::TryGetBitStringSize(str, str_len, &error_message)) {
		throw ConversionException(error_message);
	}
	return str_len;
}

uint8_t Bit::GetFirstByte(const string_t &str) {
	D_ASSERT(str.GetSize() > 1);

	auto data = const_data_ptr_cast(str.GetData());
	return data[1] & ((1 << (8 - data[0])) - 1);
}

void Bit::Finalize(string_t &str) {
	// bit strings require all padding bits to be set to 1
	// this method sets all padding bits to 1
	auto padding = GetBitPadding(str);
	for (idx_t i = 0; i < idx_t(padding); i++) {
		Bit::SetBitInternal(str, i, 1);
	}
	Bit::Verify(str);
}

void Bit::SetEmptyBitString(string_t &target, string_t &input) {
	char *res_buf = target.GetDataWriteable();
	const char *buf = input.GetData();
	memset(res_buf, 0, input.GetSize());
	res_buf[0] = buf[0];
	Bit::Finalize(target);
}

void Bit::SetEmptyBitString(string_t &target, idx_t len) {
	char *res_buf = target.GetDataWriteable();
	memset(res_buf, 0, target.GetSize());
	res_buf[0] = ComputePadding(len);
	Bit::Finalize(target);
}

// **** casting functions ****
void Bit::ToString(string_t bits, char *output) {
	auto data = const_data_ptr_cast(bits.GetData());
	auto len = bits.GetSize();

	idx_t padding = GetBitPadding(bits);
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
	auto buffer = make_unsafe_uniq_array<char>(len);
	ToString(str, buffer.get());
	return string(buffer.get(), len);
}

bool Bit::TryGetBitStringSize(string_t str, idx_t &str_len, string *error_message) {
	auto data = const_data_ptr_cast(str.GetData());
	auto len = str.GetSize();
	str_len = 0;
	for (idx_t i = 0; i < len; i++) {
		if (data[i] == '0' || data[i] == '1') {
			str_len++;
		} else {
			string error = StringUtil::Format("Invalid character encountered in string -> bit conversion: '%s'",
			                                  string(const_char_ptr_cast(data) + i, 1));
			HandleCastError::AssignError(error, error_message);
			return false;
		}
	}
	if (str_len == 0) {
		string error = "Cannot cast empty string to BIT";
		HandleCastError::AssignError(error, error_message);
		return false;
	}
	str_len = ComputeBitstringLen(str_len);
	return true;
}

void Bit::ToBit(string_t str, string_t &output_str) {
	auto data = const_data_ptr_cast(str.GetData());
	auto len = str.GetSize();
	auto output = output_str.GetDataWriteable();

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
	Bit::Finalize(output_str);
	Bit::Verify(output_str);
}

string Bit::ToBit(string_t str) {
	auto bit_len = GetBitSize(str);
	auto buffer = make_unsafe_uniq_array<char>(bit_len);
	string_t output_str(buffer.get(), bit_len);
	Bit::ToBit(str, output_str);
	return output_str.GetString();
}

void Bit::BlobToBit(string_t blob, string_t &output_str) {
	auto data = const_data_ptr_cast(blob.GetData());
	auto output = output_str.GetDataWriteable();
	idx_t size = blob.GetSize();

	*output = 0; // No padding
	memcpy(output + 1, data, size);
}

string Bit::BlobToBit(string_t blob) {
	auto buffer = make_unsafe_uniq_array<char>(blob.GetSize() + 1);
	string_t output_str(buffer.get(), blob.GetSize() + 1);
	Bit::BlobToBit(blob, output_str);
	return output_str.GetString();
}

void Bit::BitToBlob(string_t bit, string_t &output_blob) {
	D_ASSERT(bit.GetSize() == output_blob.GetSize() + 1);

	auto data = const_data_ptr_cast(bit.GetData());
	auto output = output_blob.GetDataWriteable();
	idx_t size = output_blob.GetSize();

	output[0] = GetFirstByte(bit);
	if (size > 2) {
		++output;
		// First byte in bitstring contains amount of padded bits,
		// second byte in bitstring is the padded byte,
		// therefore the rest of the data starts at data + 2 (third byte)
		memcpy(output, data + 2, size - 1);
	}
}

string Bit::BitToBlob(string_t bit) {
	D_ASSERT(bit.GetSize() > 1);

	auto buffer = make_unsafe_uniq_array<char>(bit.GetSize() - 1);
	string_t output_str(buffer.get(), bit.GetSize() - 1);
	Bit::BitToBlob(bit, output_str);
	return output_str.GetString();
}

// **** scalar functions ****
void Bit::BitString(const string_t &input, const idx_t &bit_length, string_t &result) {
	char *res_buf = result.GetDataWriteable();
	const char *buf = input.GetData();

	auto padding = ComputePadding(bit_length);
	res_buf[0] = padding;
	for (idx_t i = 0; i < bit_length; i++) {
		if (i < bit_length - input.GetSize()) {
			Bit::SetBit(result, i, 0);
		} else {
			idx_t bit = buf[i - (bit_length - input.GetSize())] == '1' ? 1 : 0;
			Bit::SetBit(result, i, bit);
		}
	}
	Bit::Finalize(result);
}

idx_t Bit::BitLength(string_t bits) {
	return ((bits.GetSize() - 1) * 8) - GetBitPadding(bits);
}

idx_t Bit::OctetLength(string_t bits) {
	return bits.GetSize() - 1;
}

idx_t Bit::BitCount(string_t bits) {
	idx_t count = 0;
	const char *buf = bits.GetData();
	for (idx_t byte_idx = 1; byte_idx < OctetLength(bits) + 1; byte_idx++) {
		for (idx_t bit_idx = 0; bit_idx < 8; bit_idx++) {
			count += (buf[byte_idx] & (1 << bit_idx)) ? 1 : 0;
		}
	}
	return count - GetBitPadding(bits);
}

idx_t Bit::BitPosition(string_t substring, string_t bits) {
	const char *buf = bits.GetData();
	auto len = bits.GetSize();
	auto substr_len = BitLength(substring);
	idx_t substr_idx = 0;

	for (idx_t bit_idx = GetBitPadding(bits); bit_idx < 8; bit_idx++) {
		idx_t bit = buf[1] & (1 << (7 - bit_idx)) ? 1 : 0;
		if (bit == GetBit(substring, substr_idx)) {
			substr_idx++;
			if (substr_idx == substr_len) {
				return (bit_idx - GetBitPadding(bits)) - substr_len + 2;
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
					return (((byte_idx - 1) * 8) + bit_idx - GetBitPadding(bits)) - substr_len + 2;
				}
			} else {
				substr_idx = 0;
			}
		}
	}
	return 0;
}

idx_t Bit::GetBit(string_t bit_string, idx_t n) {
	return Bit::GetBitInternal(bit_string, n + GetBitPadding(bit_string));
}

idx_t Bit::GetBitIndex(idx_t n) {
	return n / 8 + 1;
}

idx_t Bit::GetBitInternal(string_t bit_string, idx_t n) {
	const char *buf = bit_string.GetData();
	auto idx = Bit::GetBitIndex(n);
	D_ASSERT(idx < bit_string.GetSize());
	char byte = buf[idx] >> (7 - (n % 8));
	return (byte & 1 ? 1 : 0);
}

void Bit::SetBit(string_t &bit_string, idx_t n, idx_t new_value) {
	SetBitInternal(bit_string, n + GetBitPadding(bit_string), new_value);
}

void Bit::SetBitInternal(string_t &bit_string, idx_t n, idx_t new_value) {
	char *buf = bit_string.GetDataWriteable();

	auto idx = Bit::GetBitIndex(n);
	D_ASSERT(idx < bit_string.GetSize());
	char shift_byte = 1 << (7 - (n % 8));
	if (new_value == 0) {
		shift_byte = ~shift_byte;
		buf[idx] &= shift_byte;
	} else {
		buf[idx] |= shift_byte;
	}
}

// **** BITWISE operators ****
void Bit::RightShift(const string_t &bit_string, const idx_t &shift, string_t &result) {
	char *res_buf = result.GetDataWriteable();
	const char *buf = bit_string.GetData();
	res_buf[0] = buf[0];
	for (idx_t i = 0; i < Bit::BitLength(result); i++) {
		if (i < shift) {
			Bit::SetBit(result, i, 0);
		} else {
			idx_t bit = Bit::GetBit(bit_string, i - shift);
			Bit::SetBit(result, i, bit);
		}
	}
	Bit::Finalize(result);
}

void Bit::LeftShift(const string_t &bit_string, const idx_t &shift, string_t &result) {
	char *res_buf = result.GetDataWriteable();
	const char *buf = bit_string.GetData();
	res_buf[0] = buf[0];
	for (idx_t i = 0; i < Bit::BitLength(bit_string); i++) {
		if (i < (Bit::BitLength(bit_string) - shift)) {
			idx_t bit = Bit::GetBit(bit_string, shift + i);
			Bit::SetBit(result, i, bit);
		} else {
			Bit::SetBit(result, i, 0);
		}
	}
	Bit::Finalize(result);
	Bit::Verify(result);
}

void Bit::BitwiseAnd(const string_t &rhs, const string_t &lhs, string_t &result) {
	if (Bit::BitLength(lhs) != Bit::BitLength(rhs)) {
		throw InvalidInputException("Cannot AND bit strings of different sizes");
	}

	char *buf = result.GetDataWriteable();
	const char *r_buf = rhs.GetData();
	const char *l_buf = lhs.GetData();

	buf[0] = l_buf[0];
	for (idx_t i = 1; i < lhs.GetSize(); i++) {
		buf[i] = l_buf[i] & r_buf[i];
	}
	// and should preserve padding bits
	Bit::Verify(result);
}

void Bit::BitwiseOr(const string_t &rhs, const string_t &lhs, string_t &result) {
	if (Bit::BitLength(lhs) != Bit::BitLength(rhs)) {
		throw InvalidInputException("Cannot OR bit strings of different sizes");
	}

	char *buf = result.GetDataWriteable();
	const char *r_buf = rhs.GetData();
	const char *l_buf = lhs.GetData();

	buf[0] = l_buf[0];
	for (idx_t i = 1; i < lhs.GetSize(); i++) {
		buf[i] = l_buf[i] | r_buf[i];
	}
	// or should preserve padding bits
	Bit::Verify(result);
}

void Bit::BitwiseXor(const string_t &rhs, const string_t &lhs, string_t &result) {
	if (Bit::BitLength(lhs) != Bit::BitLength(rhs)) {
		throw InvalidInputException("Cannot XOR bit strings of different sizes");
	}

	char *buf = result.GetDataWriteable();
	const char *r_buf = rhs.GetData();
	const char *l_buf = lhs.GetData();

	buf[0] = l_buf[0];
	for (idx_t i = 1; i < lhs.GetSize(); i++) {
		buf[i] = l_buf[i] ^ r_buf[i];
	}
	Bit::Finalize(result);
}

void Bit::BitwiseNot(const string_t &input, string_t &result) {
	char *result_buf = result.GetDataWriteable();
	const char *buf = input.GetData();

	result_buf[0] = buf[0];
	for (idx_t i = 1; i < input.GetSize(); i++) {
		result_buf[i] = ~buf[i];
	}
	Bit::Finalize(result);
}

void Bit::Verify(const string_t &input) {
#ifdef DEBUG
	// bit strings require all padding bits to be set to 1
	auto padding = GetBitPadding(input);
	for (idx_t i = 0; i < padding; i++) {
		D_ASSERT(Bit::GetBitInternal(input, i));
	}
#endif
}

} // namespace duckdb
