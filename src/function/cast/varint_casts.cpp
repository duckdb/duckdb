#include "duckdb/function/cast/default_casts.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/function/cast/vector_cast_helpers.hpp"

namespace duckdb {
constexpr uint8_t VARINT_HEADER_SIZE = 3;

string_t IntToVarInt(Vector &result, int32_t int_value) {
	// Determine if the number is negative
	bool is_negative = int_value < 0;

	// Determine the number of data bytes
	uint64_t abs_value = static_cast<uint64_t>(std::abs(int_value));
	uint32_t data_byte_size = (abs_value == 0) ? 1 : static_cast<uint32_t>(std::ceil(std::log2(abs_value + 1) / 8.0));

	if (is_negative && abs_value != 0) {
		abs_value = ~abs_value;
	}

	// Create the header
	uint32_t header = data_byte_size;
	// Set MSD of 3rd byte
	header |= 0x00800000;
	if (is_negative && abs_value != 0) {
		header = ~header;
	}

	uint32_t blob_size = data_byte_size + VARINT_HEADER_SIZE;
	auto blob = StringVector::EmptyString(result, blob_size);
	if (blob_size < string_t::INLINE_BYTES) {
		// set these babies to 0.
		memset(blob.GetPrefixWriteable(), '\0', string_t::INLINE_BYTES);
	}
	auto writable_blob = blob.GetDataWriteable();
	// Add header bytes to the blob
	idx_t wb_idx = 0;
	// we ignore 4th byte of header.
	writable_blob[wb_idx++] = static_cast<char>(header >> 16 & 0xFF); // 3rd byte
	writable_blob[wb_idx++] = static_cast<char>(header >> 8 & 0xFF);  // 2nd byte
	writable_blob[wb_idx++] = static_cast<char>(header & 0xFF);       // 1st byte

	// Add data bytes to the blob
	for (int i = static_cast<int>(data_byte_size) - 1; i >= 0; --i) {
		writable_blob[wb_idx++] = static_cast<char>(abs_value >> i * 8 & 0xFF);
	}
	return blob;
}

int CharToDigit(char c) {
	return c - '0';
}

char DigitToChar(int digit) {
	return static_cast<char>(digit) + '0';
}

string_t VarcharToVarInt(Vector &result, string_t int_value) {
	if (int_value.Empty()) {
		throw InvalidInputException("bad string");
	}

	auto int_value_char = int_value.GetData();
	idx_t int_value_size = int_value.GetSize();
	idx_t start_pos = 0;

	// check if first character is -
	bool is_negative = int_value_char[0] == '-';
	if (is_negative) {
		start_pos++;
	}
	// trim 0's, unless value is 0
	if (int_value_size - start_pos > 1) {
		while (int_value_char[start_pos] == '0' && start_pos < int_value_size) {
			start_pos++;
		}
	}

	idx_t actual_size = int_value_size - start_pos;
	// convert the string to a byte array
	string abs_str(int_value_char + start_pos, actual_size);
	string blob_string;
	while (!abs_str.empty()) {
		uint8_t remainder = 0;
		std::string quotient;
		// We convert ze string to a big-endian byte array by dividing the number by 256 and storing the remainders
		for (char digit : abs_str) {
			int new_value = remainder * 10 + CharToDigit(digit);
			if (new_value / 256 > 0) {
				quotient += to_string(new_value / 256);
			} else if (!quotient.empty()) {
				quotient += '0';
			}
			remainder = static_cast<uint8_t>(new_value % 256);
		}
		if (is_negative && int_value_char[start_pos] != '0') {
			blob_string.push_back(static_cast<char>(~remainder));
		} else {
			blob_string.push_back(static_cast<char>(remainder));
		}

		// Remove leading zeros from the quotient
		abs_str = quotient;
	}
	uint32_t blob_size = static_cast<uint32_t>(blob_string.size() + VARINT_HEADER_SIZE);
	auto blob = StringVector::EmptyString(result, blob_size);
	if (blob_size < string_t::INLINE_BYTES) {
		// set these babies to 0.
		memset(blob.GetPrefixWriteable(), '\0', string_t::INLINE_BYTES);
	}
	uint32_t header = static_cast<uint32_t>(blob_string.size());
	// Set MSD of 3rd byte
	header |= 0x00800000;
	if (is_negative && int_value_char[start_pos] != '0') {
		header = ~header;
	}

	auto writable_blob = blob.GetDataWriteable();

	// Add header bytes to the blob
	writable_blob[0] = static_cast<char>(header >> 16);
	writable_blob[1] = static_cast<char>(header >> 8 & 0xFF);
	writable_blob[2] = static_cast<char>(header & 0xFF);

	// Write string_blob into blob
	idx_t blob_string_idx = blob_string.size() - 1;
	for (idx_t i = VARINT_HEADER_SIZE; i < blob_size; i++) {
		writable_blob[i] = blob_string[blob_string_idx--];
	}
	return blob;
}

// Function to convert VARINT blob to a VARCHAR
string_t VarIntToVarchar(const string_t &blob) {
	if (blob.GetSize() < 4) {
		throw InvalidInputException("Invalid blob size.");
	}
	auto blob_ptr = blob.GetData();
	std::string decimalString;
	std::vector<uint8_t> tempArray;
	// Determine if the number is negative
	bool is_negative = (blob_ptr[0] & 0x80) == 0;
	for (idx_t i = 3; i < blob.GetSize(); i++) {
		if (is_negative) {
			tempArray.push_back(static_cast<uint8_t>(~blob_ptr[i]));
		} else {
			tempArray.push_back(static_cast<uint8_t>(blob_ptr[i]));
		}
	}
	std::reverse(decimalString.begin(), decimalString.end());

	while (!tempArray.empty()) {
		std::string quotient;
		uint8_t remainder = 0;
		for (uint8_t byte : tempArray) {
			int new_value = remainder * 256 + byte;
			quotient += DigitToChar(new_value / 10);
			remainder = static_cast<uint8_t>(new_value % 10);
		}

		decimalString += DigitToChar(remainder);

		// Remove leading zeros from the quotient
		tempArray.clear();
		for (char digit : quotient) {
			if (digit != '0' || !tempArray.empty()) {
				tempArray.push_back(static_cast<uint8_t>(CharToDigit(digit)));
			}
		}
	}

	if (is_negative) {
		decimalString += '-';
	}

	// Reverse the string to get the correct decimal representation
	std::reverse(decimalString.begin(), decimalString.end());
	return decimalString;
}

struct IntTryCastToVarInt {
	template <class SRC>
	static inline string_t Operation(SRC input, Vector &result) {
		return IntToVarInt(result, input);
	}
};

struct VarcharTryCastToVarInt {
	template <class SRC>
	static inline string_t Operation(SRC input, Vector &result) {
		return VarcharToVarInt(result, input);
	}
};

struct VarIntTryCastToVarchar {
	template <class SRC>
	static inline string_t Operation(SRC input, Vector &result) {
		return StringVector::AddStringOrBlob(result, VarIntToVarchar(input));
	}
};

BoundCastInfo DefaultCasts::ToVarintCastSwitch(BindCastInput &input, const LogicalType &source,
                                               const LogicalType &target) {
	D_ASSERT(target.id() == LogicalTypeId::VARINT);
	// now switch on the result type
	switch (source.id()) {
	case LogicalTypeId::INTEGER:
		return BoundCastInfo(&VectorCastHelpers::StringCast<int32_t, duckdb::IntTryCastToVarInt>);
	case LogicalTypeId::VARCHAR:
		return BoundCastInfo(&VectorCastHelpers::StringCast<string_t, duckdb::VarcharTryCastToVarInt>);
	case LogicalTypeId::DOUBLE:
		return TryVectorNullCast;
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
		return BoundCastInfo(&VectorCastHelpers::StringCast<string_t, duckdb::VarIntTryCastToVarchar>);
	default:
		return TryVectorNullCast;
	}
}

} // namespace duckdb
