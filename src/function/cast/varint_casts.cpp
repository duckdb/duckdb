#include "duckdb/function/cast/default_casts.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/function/cast/vector_cast_helpers.hpp"
#include "duckdb/common/types/varint.hpp"
#include <cmath>

namespace duckdb {

template <class T>
string_t IntToVarInt(Vector &result, T int_value) {
	// Determine if the number is negative
	bool is_negative = int_value < 0;
	// Determine the number of data bytes
	uint64_t abs_value;
	if (is_negative) {
		if (int_value == std::numeric_limits<T>::min()) {
			abs_value = static_cast<uint64_t>(std::numeric_limits<T>::max()) + 1;
		} else {
			abs_value = static_cast<uint64_t>(std::abs(static_cast<int64_t>(int_value)));
		}
	} else {
		abs_value = static_cast<uint64_t>(int_value);
	}
	uint32_t data_byte_size;
	if (abs_value != NumericLimits<uint64_t>::Maximum()) {
		data_byte_size = (abs_value == 0) ? 1 : static_cast<uint32_t>(std::ceil(std::log2(abs_value + 1) / 8.0));
	} else {
		data_byte_size = static_cast<uint32_t>(std::ceil(std::log2(abs_value) / 8.0));
	}

	uint32_t blob_size = data_byte_size + Varint::VARINT_HEADER_SIZE;
	auto blob = StringVector::EmptyString(result, blob_size);
	auto writable_blob = blob.GetDataWriteable();
	Varint::SetHeader(writable_blob, data_byte_size, is_negative);

	// Add data bytes to the blob, starting off after header bytes
	idx_t wb_idx = Varint::VARINT_HEADER_SIZE;
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

template <>
string_t HugeintCastToVarInt::Operation(hugeint_t int_value, Vector &result) {
	// Determine if the number is negative
	bool is_negative = (int_value.upper >> 63) & 1;
	if (is_negative) {
		int_value.lower = ~int_value.lower + 1;
		int_value.upper = ~int_value.upper;
	}
	// Determine the number of data bytes
	uint64_t abs_value_upper = static_cast<uint64_t>(int_value.upper);

	uint32_t data_byte_size;
	if (abs_value_upper != NumericLimits<uint64_t>::Maximum()) {
		data_byte_size =
		    (abs_value_upper == 0) ? 0 : static_cast<uint32_t>(std::ceil(std::log2(abs_value_upper + 1) / 8.0));
	} else {
		data_byte_size = static_cast<uint32_t>(std::ceil(std::log2(abs_value_upper) / 8.0));
	}

	uint32_t upper_byte_size = data_byte_size;
	if (int_value.lower != NumericLimits<uint64_t>::Maximum()) {
		data_byte_size += static_cast<uint32_t>(std::ceil(std::log2(int_value.lower + 1) / 8.0));
	} else {
		data_byte_size += static_cast<uint32_t>(std::ceil(std::log2(int_value.lower) / 8.0));
	}
	if (data_byte_size == 0) {
		data_byte_size++;
	}
	uint32_t blob_size = data_byte_size + Varint::VARINT_HEADER_SIZE;
	auto blob = StringVector::EmptyString(result, blob_size);
	auto writable_blob = blob.GetDataWriteable();
	Varint::SetHeader(writable_blob, data_byte_size, is_negative);

	// Add data bytes to the blob, starting off after header bytes
	idx_t wb_idx = Varint::VARINT_HEADER_SIZE;
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
template <>
string_t HugeintCastToVarInt::Operation(uhugeint_t int_value, Vector &result) {
	uint32_t data_byte_size;
	if (int_value.upper != NumericLimits<uint64_t>::Maximum()) {
		data_byte_size =
		    (int_value.upper == 0) ? 0 : static_cast<uint32_t>(std::ceil(std::log2(int_value.upper + 1) / 8.0));
	} else {
		data_byte_size = static_cast<uint32_t>(std::ceil(std::log2(int_value.upper) / 8.0));
	}

	uint32_t upper_byte_size = data_byte_size;
	if (int_value.lower != NumericLimits<uint64_t>::Maximum()) {
		data_byte_size += static_cast<uint32_t>(std::ceil(std::log2(int_value.lower + 1) / 8.0));
	} else {
		data_byte_size += static_cast<uint32_t>(std::ceil(std::log2(int_value.lower) / 8.0));
	}
	if (data_byte_size == 0) {
		data_byte_size++;
	}
	uint32_t blob_size = data_byte_size + Varint::VARINT_HEADER_SIZE;
	auto blob = StringVector::EmptyString(result, blob_size);
	auto writable_blob = blob.GetDataWriteable();
	Varint::SetHeader(writable_blob, data_byte_size, false);

	// Add data bytes to the blob, starting off after header bytes
	idx_t wb_idx = Varint::VARINT_HEADER_SIZE;
	for (int i = static_cast<int>(upper_byte_size) - 1; i >= 0; --i) {
		writable_blob[wb_idx++] = static_cast<char>(int_value.upper >> i * 8 & 0xFF);
	}
	for (int i = static_cast<int>(data_byte_size - upper_byte_size) - 1; i >= 0; --i) {
		writable_blob[wb_idx++] = static_cast<char>(int_value.lower >> i * 8 & 0xFF);
	}
	blob.Finalize();
	return blob;
}

// Varchar to Varint
// TODO: This is a slow quadratic algorithm, we can still optimize it further.
template <>
bool TryCastToVarInt::Operation(string_t input_value, string_t &result_value, Vector &result,
                                CastParameters &parameters) {
	idx_t start_pos, end_pos;
	bool is_negative, is_zero;
	if (!Varint::VarcharFormatting(input_value, start_pos, end_pos, is_negative, is_zero)) {
		return false;
	}
	if (is_zero) {
		// Return Value 0
		result_value = Varint::InitializeVarintZero(result);
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

	uint32_t blob_size = static_cast<uint32_t>(blob_string.size() + Varint::VARINT_HEADER_SIZE);
	result_value = StringVector::EmptyString(result, blob_size);
	auto writable_blob = result_value.GetDataWriteable();

	Varint::SetHeader(writable_blob, blob_string.size(), is_negative);

	// Write string_blob into blob
	idx_t blob_string_idx = blob_string.size() - 1;
	for (idx_t i = Varint::VARINT_HEADER_SIZE; i < blob_size; i++) {
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
		result_value = Varint::InitializeVarintZero(result);
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
	uint32_t blob_size = data_byte_size + Varint::VARINT_HEADER_SIZE;
	result_value = StringVector::EmptyString(result, blob_size);
	auto writable_blob = result_value.GetDataWriteable();
	Varint::SetHeader(writable_blob, data_byte_size, is_negative);
	// Add data bytes to the blob, starting off after header bytes
	idx_t blob_string_idx = value.size() - 1;
	for (idx_t i = Varint::VARINT_HEADER_SIZE; i < blob_size; i++) {
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

BoundCastInfo Varint::NumericToVarintCastSwitch(const LogicalType &source) {
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
	case LogicalTypeId::UHUGEINT:
		return BoundCastInfo(&VectorCastHelpers::StringCast<uhugeint_t, HugeintCastToVarInt>);
	case LogicalTypeId::FLOAT:
		return BoundCastInfo(&VectorCastHelpers::TryCastStringLoop<float, string_t, TryCastToVarInt>);
	case LogicalTypeId::DOUBLE:
		return BoundCastInfo(&VectorCastHelpers::TryCastStringLoop<double, string_t, TryCastToVarInt>);
	case LogicalTypeId::HUGEINT:
		return BoundCastInfo(&VectorCastHelpers::StringCast<hugeint_t, HugeintCastToVarInt>);
	case LogicalTypeId::DECIMAL:
	default:
		return DefaultCasts::TryVectorNullCast;
	}
}

BoundCastInfo DefaultCasts::VarintCastSwitch(BindCastInput &input, const LogicalType &source,
                                             const LogicalType &target) {
	D_ASSERT(source.id() == LogicalTypeId::VARINT);
	// now switch on the result type
	switch (target.id()) {
	case LogicalTypeId::VARCHAR:
		return BoundCastInfo(&VectorCastHelpers::StringCast<string_t, VarIntCastToVarchar>);
	case LogicalTypeId::DOUBLE:
		return BoundCastInfo(&VectorCastHelpers::TryCastLoop<string_t, double, VarintToDoubleCast>);
	default:
		return TryVectorNullCast;
	}
}

} // namespace duckdb
