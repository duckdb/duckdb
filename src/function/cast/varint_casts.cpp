#include "duckdb/function/cast/default_casts.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/function/cast/vector_cast_helpers.hpp"
#include <cmath>

namespace duckdb {

constexpr uint8_t VARINT_HEADER_SIZE = 3;

void SetHeader(char *blob, uint64_t number_of_bytes, bool is_negative) {
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
string_t ZeroBlob(Vector &result) {
	// return zero
	uint32_t blob_size = 1 + VARINT_HEADER_SIZE;
	auto blob = StringVector::EmptyString(result, blob_size);
	auto writable_blob = blob.GetDataWriteable();
	SetHeader(writable_blob, 1, false);
	writable_blob[3] = 0;
	blob.Finalize();
	return blob;
}
template <class T>
string_t IntToVarInt(Vector &result, T int_value) {
	// Determine if the number is negative
	bool is_negative = int_value < 0;
	// Determine the number of data bytes
	uint64_t abs_value;
	if (is_negative) {
		abs_value = static_cast<uint64_t>(std::abs(static_cast<int64_t>(int_value)));
	} else {
		abs_value = static_cast<uint64_t>(int_value);
	}
	uint32_t data_byte_size;
	if (abs_value != NumericLimits<uint64_t>::Maximum()) {
		data_byte_size = (abs_value == 0) ? 1 : static_cast<uint32_t>(std::ceil(std::log2(abs_value + 1) / 8.0));
	} else {
		data_byte_size = static_cast<uint32_t>(std::ceil(std::log2(abs_value) / 8.0));
	}

	uint32_t blob_size = data_byte_size + VARINT_HEADER_SIZE;
	auto blob = StringVector::EmptyString(result, blob_size);
	auto writable_blob = blob.GetDataWriteable();
	SetHeader(writable_blob, data_byte_size, is_negative);

	// Add data bytes to the blob, starting off after header bytes
	idx_t wb_idx = VARINT_HEADER_SIZE;
	for (int i = static_cast<int>(data_byte_size) - 1; i >= 0; --i) {
		if (is_negative) {
			writable_blob[wb_idx++] = ~static_cast<char>(abs_value >> i * 8 & 0xFF);
		} else {
			writable_blob[wb_idx++] = static_cast<char>(abs_value >> i * 8 & 0xFF);
		}
	}
	blob.Finalize();
	return blob;
}

template <class T>
string_t HugeintToVarInt(Vector &result, T int_value) {
	// Determine if the number is negative
	bool is_negative = int_value.upper < 0;
	// Determine the number of data bytes
	uint64_t abs_value_upper;
	if (is_negative) {
		abs_value_upper = static_cast<uint64_t>(std::abs(static_cast<int64_t>(int_value.upper)));
	} else {
		abs_value_upper = static_cast<uint64_t>(int_value.upper);
	}
	uint32_t data_byte_size;
	if (abs_value_upper != NumericLimits<uint64_t>::Maximum()) {
		data_byte_size =
		    (abs_value_upper == 0) ? 0 : static_cast<uint32_t>(std::ceil(std::log2(abs_value_upper + 1) / 8.0));
	} else {
		data_byte_size = static_cast<uint32_t>(std::ceil(std::log2(abs_value_upper) / 8.0));
	}

	uint32_t upper_byte_size = data_byte_size;
	if (abs_value_upper != NumericLimits<uint64_t>::Maximum()) {
		data_byte_size += static_cast<uint32_t>(std::ceil(std::log2(int_value.lower + 1) / 8.0));
	} else {
		data_byte_size += static_cast<uint32_t>(std::ceil(std::log2(int_value.lower) / 8.0));
	}
	if (data_byte_size == 0) {
		data_byte_size++;
	}
	uint32_t blob_size = data_byte_size + VARINT_HEADER_SIZE;
	auto blob = StringVector::EmptyString(result, blob_size);
	auto writable_blob = blob.GetDataWriteable();
	SetHeader(writable_blob, data_byte_size, is_negative);

	// Add data bytes to the blob, starting off after header bytes
	idx_t wb_idx = VARINT_HEADER_SIZE;
	for (int i = static_cast<int>(upper_byte_size) - 1; i >= 0; --i) {
		if (is_negative) {
			writable_blob[wb_idx++] = ~static_cast<char>(abs_value_upper >> i * 8 & 0xFF);
		} else {
			writable_blob[wb_idx++] = static_cast<char>(abs_value_upper >> i * 8 & 0xFF);
		}
	}
	for (int i = static_cast<int>(data_byte_size - upper_byte_size) - 1; i >= 0; --i) {
		if (is_negative) {
			writable_blob[wb_idx++] = ~static_cast<char>(int_value.lower >> i * 8 & 0xFF);
		} else {
			writable_blob[wb_idx++] = static_cast<char>(int_value.lower >> i * 8 & 0xFF);
		}
	}
	blob.Finalize();
	return blob;
}

int CharToDigit(char c) {
	return c - '0';
}

char DigitToChar(int digit) {
	return static_cast<char>(digit) + '0';
}

// Function to prepare a varchar for conversion
// We trim zero's, check for negative values, and what-not
// Returns false if this is an invalid varchar
bool VarcharFormatting(string_t &value, idx_t &start_pos, idx_t &end_pos, bool &is_negative, bool &is_zero) {
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

void GetByteArray(vector<uint8_t> &byte_array, bool &is_negative, const string_t &blob) {
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

// Function to convert VARINT blob to a VARCHAR
string VarIntToVarchar(const string_t &blob) {
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

struct IntCastToVarInt {
	template <class SRC>
	static inline string_t Operation(SRC input, Vector &result) {
		return IntToVarInt(result, input);
	}
};

struct HugeintCastToVarInt {
	template <class SRC>
	static inline string_t Operation(SRC input, Vector &result) {
		return HugeintToVarInt(result, input);
	}
};

struct TryCastToVarInt {
	template <class SRC, class DST>
	static inline bool Operation(SRC input, DST &result, Vector &result_vector, CastParameters &parameters) {
		return VarcharToVarInt(result, input, result_vector);
	}
};

// Varchar to Varint
// TODO: This is a slow quadratic algorithm, we can still optimize it further.
template <>
bool TryCastToVarInt::Operation(string_t input_value, string_t &result_value, Vector &result,
                                CastParameters &parameters) {
	idx_t start_pos, end_pos;
	bool is_negative, is_zero;
	if (!VarcharFormatting(input_value, start_pos, end_pos, is_negative, is_zero)) {
		return false;
	}
	if (is_zero) {
		// Return Value 0
		result_value = ZeroBlob(result);
		return true;
	}
	auto int_value_char = input_value.GetData();
	idx_t actual_size = end_pos - start_pos;
	// convert the string to a byte array
	string blob_string;

	unsafe_vector<uint64_t> digits;

	// The max number a uint64_t can represent is 18.446.744.073.709.551.615
	// That has 20 digits
	// In the worst case a remainder of a division will be 255, which is 3 digits
	// Since the max value is 184, we need to take one more digit out
	// Hence we end up with a max of 16 digits supported.
	const uint8_t max_digits = 16;
	const idx_t number_of_digits = static_cast<idx_t>(std::ceil((double)actual_size / max_digits));

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
			blob_string.push_back(static_cast<char>(~remainder));
		} else {
			blob_string.push_back(static_cast<char>(remainder));
		}
	}

	uint32_t blob_size = static_cast<uint32_t>(blob_string.size() + VARINT_HEADER_SIZE);
	result_value = StringVector::EmptyString(result, blob_size);
	auto writable_blob = result_value.GetDataWriteable();

	SetHeader(writable_blob, blob_string.size(), is_negative);

	// Write string_blob into blob
	idx_t blob_string_idx = blob_string.size() - 1;
	for (idx_t i = VARINT_HEADER_SIZE; i < blob_size; i++) {
		writable_blob[i] = blob_string[blob_string_idx--];
	}
	result_value.Finalize();
	return true;
}

template <class T>
bool DoubleToVarInt(T double_value, string_t &result_value, Vector &result) {
	// Check if we can cast it
	if (!std::isfinite(double_value)) {
		// We can't cast inf -inf nan
		return false;
	}
	// Determine if the number is negative
	bool is_negative = double_value < 0;
	// Determine the number of data bytes
	double abs_value = std::abs(double_value);

	if (abs_value == 0) {
		// Return Value 0
		result_value = ZeroBlob(result);
		return true;
	}
	vector<char> value;
	while (abs_value > 0) {
		double quotient = abs_value / 256;
		double truncated = floor(quotient);
		uint8_t byte = static_cast<uint8_t>(abs_value - truncated * 256);
		abs_value = truncated;
		if (is_negative) {
			value.push_back(~static_cast<char>(byte));
		} else {
			value.push_back(static_cast<char>(byte));
		}
	}
	uint32_t data_byte_size = static_cast<uint32_t>(value.size());
	uint32_t blob_size = data_byte_size + VARINT_HEADER_SIZE;
	result_value = StringVector::EmptyString(result, blob_size);
	auto writable_blob = result_value.GetDataWriteable();
	SetHeader(writable_blob, data_byte_size, is_negative);
	// Add data bytes to the blob, starting off after header bytes
	idx_t blob_string_idx = value.size() - 1;
	for (idx_t i = VARINT_HEADER_SIZE; i < blob_size; i++) {
		writable_blob[i] = value[blob_string_idx--];
	}
	result_value.Finalize();
	return true;
}

template <>
bool TryCastToVarInt::Operation(double double_value, string_t &result_value, Vector &result,
                                CastParameters &parameters) {
	return DoubleToVarInt(double_value, result_value, result);
}

template <>
bool TryCastToVarInt::Operation(float double_value, string_t &result_value, Vector &result,
                                CastParameters &parameters) {
	return DoubleToVarInt(double_value, result_value, result);
}

struct VarIntTryCastToVarchar {
	template <class SRC>
	static inline string_t Operation(SRC input, Vector &result) {
		return StringVector::AddStringOrBlob(result, VarIntToVarchar(input));
	}
};

bool VarintToDouble(string_t &blob, double &result, bool &strict) {
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

struct VarintToDoubleCast {
	template <class SRC, class DST>
	static inline bool Operation(SRC input, DST &result, bool strict = false) {
		return VarintToDouble(input, result, strict);
	}
};

BoundCastInfo DefaultCasts::ToVarintCastSwitch(BindCastInput &input, const LogicalType &source,
                                               const LogicalType &target) {
	D_ASSERT(target.id() == LogicalTypeId::VARINT);
	// now switch on the result type
	switch (source.id()) {
	case LogicalTypeId::TINYINT:
		return BoundCastInfo(&VectorCastHelpers::StringCast<int8_t, IntCastToVarInt>);
	case LogicalTypeId::UTINYINT:
		return BoundCastInfo(&VectorCastHelpers::StringCast<uint8_t, IntCastToVarInt>);
	case LogicalTypeId::SMALLINT:
		return BoundCastInfo(&VectorCastHelpers::StringCast<int16_t, IntCastToVarInt>);
	case LogicalTypeId::USMALLINT:
		return BoundCastInfo(&VectorCastHelpers::StringCast<uint16_t, IntCastToVarInt>);
	case LogicalTypeId::INTEGER:
		return BoundCastInfo(&VectorCastHelpers::StringCast<int32_t, IntCastToVarInt>);
	case LogicalTypeId::UINTEGER:
		return BoundCastInfo(&VectorCastHelpers::StringCast<uint32_t, IntCastToVarInt>);
	case LogicalTypeId::BIGINT:
		return BoundCastInfo(&VectorCastHelpers::StringCast<int64_t, IntCastToVarInt>);
	case LogicalTypeId::UBIGINT:
		return BoundCastInfo(&VectorCastHelpers::StringCast<uint64_t, IntCastToVarInt>);
	case LogicalTypeId::VARCHAR:
		return BoundCastInfo(&VectorCastHelpers::TryCastStringLoop<string_t, string_t, TryCastToVarInt>);
	case LogicalTypeId::UHUGEINT:
		return BoundCastInfo(&VectorCastHelpers::StringCast<uhugeint_t, HugeintCastToVarInt>);
	case LogicalTypeId::FLOAT:
		return BoundCastInfo(&VectorCastHelpers::TryCastStringLoop<float, string_t, TryCastToVarInt>);
	case LogicalTypeId::DOUBLE:
		return BoundCastInfo(&VectorCastHelpers::TryCastStringLoop<double, string_t, TryCastToVarInt>);
	case LogicalTypeId::HUGEINT:
	case LogicalTypeId::DECIMAL:
	default:
		return TryVectorNullCast;
	}
}

BoundCastInfo DefaultCasts::VarintCastSwitch(BindCastInput &input, const LogicalType &source,
                                             const LogicalType &target) {
	D_ASSERT(source.id() == LogicalTypeId::VARINT);
	// now switch on the result type
	switch (target.id()) {
	case LogicalTypeId::VARCHAR:
		return BoundCastInfo(&VectorCastHelpers::StringCast<string_t, VarIntTryCastToVarchar>);
	case LogicalTypeId::DOUBLE:
		return BoundCastInfo(&VectorCastHelpers::TryCastLoop<string_t, double, VarintToDoubleCast>);
	default:
		return TryVectorNullCast;
	}
}

} // namespace duckdb
