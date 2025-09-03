#include "duckdb/function/cast/default_casts.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/function/cast/vector_cast_helpers.hpp"
#include "duckdb/common/types/bignum.hpp"
#include <cmath>
#include "duckdb/common/bignum.hpp"

namespace duckdb {

template <class T>
static bignum_t IntToBignum(Vector &result, T int_value) {
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

	uint32_t blob_size = data_byte_size + Bignum::BIGNUM_HEADER_SIZE;
	auto blob = StringVector::EmptyString(result, blob_size);
	auto writable_blob = blob.GetDataWriteable();
	Bignum::SetHeader(writable_blob, data_byte_size, is_negative);

	// Add data bytes to the blob, starting off after header bytes
	idx_t wb_idx = Bignum::BIGNUM_HEADER_SIZE;
	for (int i = static_cast<int>(data_byte_size) - 1; i >= 0; --i) {
		if (is_negative) {
			writable_blob[wb_idx++] = static_cast<char>(~(abs_value >> i * 8 & 0xFF));
		} else {
			writable_blob[wb_idx++] = static_cast<char>(abs_value >> i * 8 & 0xFF);
		}
	}
	blob.Finalize();
	bignum_t result_bignum(blob);
	return result_bignum;
}

template <>
bignum_t HugeintCastToBignum::Operation(uhugeint_t int_value, Vector &result) {
	uint32_t data_byte_size;
	if (int_value.upper != NumericLimits<uint64_t>::Maximum()) {
		data_byte_size =
		    (int_value.upper == 0) ? 0 : static_cast<uint32_t>(std::ceil(std::log2(int_value.upper + 1) / 8.0));
	} else {
		data_byte_size = static_cast<uint32_t>(std::ceil(std::log2(int_value.upper) / 8.0));
	}

	uint32_t upper_byte_size = data_byte_size;
	if (data_byte_size > 0) {
		// If we have at least one byte on the upper side, the bottom side is complete
		data_byte_size += 8;
	} else {
		if (int_value.lower != NumericLimits<uint64_t>::Maximum()) {
			data_byte_size += static_cast<uint32_t>(std::ceil(std::log2(int_value.lower + 1) / 8.0));
		} else {
			data_byte_size += static_cast<uint32_t>(std::ceil(std::log2(int_value.lower) / 8.0));
		}
	}
	if (data_byte_size == 0) {
		data_byte_size++;
	}
	uint32_t blob_size = data_byte_size + Bignum::BIGNUM_HEADER_SIZE;
	auto blob = StringVector::EmptyString(result, blob_size);
	auto writable_blob = blob.GetDataWriteable();
	Bignum::SetHeader(writable_blob, data_byte_size, false);

	// Add data bytes to the blob, starting off after header bytes
	idx_t wb_idx = Bignum::BIGNUM_HEADER_SIZE;
	for (int i = static_cast<int>(upper_byte_size) - 1; i >= 0; --i) {
		writable_blob[wb_idx++] = static_cast<char>(int_value.upper >> i * 8 & 0xFF);
	}
	for (int i = static_cast<int>(data_byte_size - upper_byte_size) - 1; i >= 0; --i) {
		writable_blob[wb_idx++] = static_cast<char>(int_value.lower >> i * 8 & 0xFF);
	}
	blob.Finalize();
	bignum_t result_bignum(blob);
	return result_bignum;
}

template <>
bignum_t HugeintCastToBignum::Operation(hugeint_t int_value, Vector &result) {
	// Determine if the number is negative
	bool is_negative = int_value.upper >> 63 & 1;
	if (is_negative) {
		// We must check if it's -170141183460469231731687303715884105728, since it's not possible to negate it
		// without overflowing
		if (int_value == NumericLimits<hugeint_t>::Minimum()) {
			uhugeint_t u_int_value {0x8000000000000000, 0};
			auto cast_value = Operation<uhugeint_t>(u_int_value, result);
			// We have to do all the bit flipping.
			auto writable_value_ptr = cast_value.data.GetDataWriteable();
			Bignum::SetHeader(writable_value_ptr, cast_value.data.GetSize() - Bignum::BIGNUM_HEADER_SIZE, is_negative);
			for (idx_t i = Bignum::BIGNUM_HEADER_SIZE; i < cast_value.data.GetSize(); i++) {
				writable_value_ptr[i] = static_cast<char>(~writable_value_ptr[i]);
			}
			cast_value.data.Finalize();
			return cast_value;
		}
		int_value = -int_value;
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
	if (data_byte_size > 0) {
		// If we have at least one byte on the upper side, the bottom side is complete
		data_byte_size += 8;
	} else {
		if (int_value.lower != NumericLimits<uint64_t>::Maximum()) {
			data_byte_size += static_cast<uint32_t>(std::ceil(std::log2(int_value.lower + 1) / 8.0));
		} else {
			data_byte_size += static_cast<uint32_t>(std::ceil(std::log2(int_value.lower) / 8.0));
		}
	}

	if (data_byte_size == 0) {
		data_byte_size++;
	}
	uint32_t blob_size = data_byte_size + Bignum::BIGNUM_HEADER_SIZE;
	auto blob = StringVector::EmptyString(result, blob_size);
	auto writable_blob = blob.GetDataWriteable();
	Bignum::SetHeader(writable_blob, data_byte_size, is_negative);

	// Add data bytes to the blob, starting off after header bytes
	idx_t wb_idx = Bignum::BIGNUM_HEADER_SIZE;
	for (int i = static_cast<int>(upper_byte_size) - 1; i >= 0; --i) {
		if (is_negative) {
			writable_blob[wb_idx++] = static_cast<char>(~(abs_value_upper >> i * 8 & 0xFF));
		} else {
			writable_blob[wb_idx++] = static_cast<char>(abs_value_upper >> i * 8 & 0xFF);
		}
	}
	for (int i = static_cast<int>(data_byte_size - upper_byte_size) - 1; i >= 0; --i) {
		if (is_negative) {
			writable_blob[wb_idx++] = static_cast<char>(~(int_value.lower >> i * 8 & 0xFF));
		} else {
			writable_blob[wb_idx++] = static_cast<char>(int_value.lower >> i * 8 & 0xFF);
		}
	}
	blob.Finalize();
	bignum_t result_bignum(blob);
	return result_bignum;
}

// Varchar to Bignum
template <>
bool TryCastToBignum::Operation(string_t input_value, bignum_t &result_value, Vector &result,
                                CastParameters &parameters) {
	auto blob_string = Bignum::VarcharToBignum(input_value);

	uint32_t blob_size = static_cast<uint32_t>(blob_string.size());
	result_value = bignum_t(StringVector::EmptyString(result, blob_size));
	auto writable_blob = result_value.data.GetDataWriteable();

	// Write string_blob into blob
	for (idx_t i = 0; i < blob_string.size(); i++) {
		writable_blob[i] = blob_string[i];
	}
	result_value.data.Finalize();
	return true;
}

template <class T>
static bool DoubleToBignum(T double_value, bignum_t &result_value, Vector &result) {
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
		result_value = Bignum::InitializeBignumZero(result);
		return true;
	}
	vector<char> value;
	while (abs_value > 0) {
		double quotient = abs_value / 256;
		double truncated = floor(quotient);
		uint8_t byte = static_cast<uint8_t>(abs_value - truncated * 256);
		abs_value = truncated;
		if (is_negative) {
			value.push_back(static_cast<char>(~byte));
		} else {
			value.push_back(static_cast<char>(byte));
		}
	}
	uint32_t data_byte_size = static_cast<uint32_t>(value.size());
	uint32_t blob_size = data_byte_size + Bignum::BIGNUM_HEADER_SIZE;
	result_value.data = StringVector::EmptyString(result, blob_size);
	auto writable_blob = result_value.data.GetDataWriteable();
	Bignum::SetHeader(writable_blob, data_byte_size, is_negative);
	// Add data bytes to the blob, starting off after header bytes
	idx_t blob_string_idx = value.size() - 1;
	for (idx_t i = Bignum::BIGNUM_HEADER_SIZE; i < blob_size; i++) {
		writable_blob[i] = value[blob_string_idx--];
	}
	result_value.data.Finalize();
	return true;
}

template <>
bool TryCastToBignum::Operation(double double_value, bignum_t &result_value, Vector &result,
                                CastParameters &parameters) {
	return DoubleToBignum(double_value, result_value, result);
}

template <>
bool TryCastToBignum::Operation(float double_value, bignum_t &result_value, Vector &result,
                                CastParameters &parameters) {
	return DoubleToBignum(double_value, result_value, result);
}

BoundCastInfo Bignum::NumericToBignumCastSwitch(const LogicalType &source) {
	// now switch on the result type
	switch (source.id()) {
	case LogicalTypeId::TINYINT:
		return BoundCastInfo(&VectorCastHelpers::StringCast<int8_t, IntCastToBignum, bignum_t>);
	case LogicalTypeId::UTINYINT:
		return BoundCastInfo(&VectorCastHelpers::StringCast<uint8_t, IntCastToBignum, bignum_t>);
	case LogicalTypeId::SMALLINT:
		return BoundCastInfo(&VectorCastHelpers::StringCast<int16_t, IntCastToBignum, bignum_t>);
	case LogicalTypeId::USMALLINT:
		return BoundCastInfo(&VectorCastHelpers::StringCast<uint16_t, IntCastToBignum, bignum_t>);
	case LogicalTypeId::INTEGER:
		return BoundCastInfo(&VectorCastHelpers::StringCast<int32_t, IntCastToBignum, bignum_t>);
	case LogicalTypeId::UINTEGER:
		return BoundCastInfo(&VectorCastHelpers::StringCast<uint32_t, IntCastToBignum, bignum_t>);
	case LogicalTypeId::BIGINT:
		return BoundCastInfo(&VectorCastHelpers::StringCast<int64_t, IntCastToBignum, bignum_t>);
	case LogicalTypeId::UBIGINT:
		return BoundCastInfo(&VectorCastHelpers::StringCast<uint64_t, IntCastToBignum, bignum_t>);
	case LogicalTypeId::UHUGEINT:
		return BoundCastInfo(&VectorCastHelpers::StringCast<uhugeint_t, HugeintCastToBignum, bignum_t>);
	case LogicalTypeId::FLOAT:
		return BoundCastInfo(&VectorCastHelpers::TryCastStringLoop<float, bignum_t, TryCastToBignum>);
	case LogicalTypeId::DOUBLE:
		return BoundCastInfo(&VectorCastHelpers::TryCastStringLoop<double, bignum_t, TryCastToBignum>);
	case LogicalTypeId::HUGEINT:
		return BoundCastInfo(&VectorCastHelpers::StringCast<hugeint_t, HugeintCastToBignum, bignum_t>);
	case LogicalTypeId::DECIMAL:
	default:
		return DefaultCasts::TryVectorNullCast;
	}
}

BoundCastInfo DefaultCasts::BignumCastSwitch(BindCastInput &input, const LogicalType &source,
                                             const LogicalType &target) {
	D_ASSERT(source.id() == LogicalTypeId::BIGNUM);
	// now switch on the result type
	switch (target.id()) {
	case LogicalTypeId::TINYINT:
		return BoundCastInfo(&VectorCastHelpers::TryCastLoop<bignum_t, int8_t, BignumToIntCast>);
	case LogicalTypeId::UTINYINT:
		return BoundCastInfo(&VectorCastHelpers::TryCastLoop<bignum_t, uint8_t, BignumToIntCast>);

	case LogicalTypeId::SMALLINT:
		return BoundCastInfo(&VectorCastHelpers::TryCastLoop<bignum_t, int16_t, BignumToIntCast>);

	case LogicalTypeId::USMALLINT:
		return BoundCastInfo(&VectorCastHelpers::TryCastLoop<bignum_t, uint16_t, BignumToIntCast>);

	case LogicalTypeId::INTEGER:
		return BoundCastInfo(&VectorCastHelpers::TryCastLoop<bignum_t, int32_t, BignumToIntCast>);

	case LogicalTypeId::UINTEGER:
		return BoundCastInfo(&VectorCastHelpers::TryCastLoop<bignum_t, uint32_t, BignumToIntCast>);

	case LogicalTypeId::BIGINT:
		return BoundCastInfo(&VectorCastHelpers::TryCastLoop<bignum_t, int64_t, BignumToIntCast>);

	case LogicalTypeId::UBIGINT:
		return BoundCastInfo(&VectorCastHelpers::TryCastLoop<bignum_t, uint64_t, BignumToIntCast>);

	case LogicalTypeId::HUGEINT:
		return BoundCastInfo(&VectorCastHelpers::TryCastLoop<bignum_t, hugeint_t, BignumToIntCast>);

	case LogicalTypeId::UHUGEINT:
		return BoundCastInfo(&VectorCastHelpers::TryCastLoop<bignum_t, uhugeint_t, BignumToIntCast>);

	case LogicalTypeId::VARCHAR:
		return BoundCastInfo(&VectorCastHelpers::StringCast<bignum_t, BignumCastToVarchar>);
	case LogicalTypeId::DOUBLE:
		return BoundCastInfo(&VectorCastHelpers::TryCastLoop<bignum_t, double, BignumToDoubleCast>);
	default:
		return TryVectorNullCast;
	}
}

} // namespace duckdb
