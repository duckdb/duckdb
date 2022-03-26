#include "duckdb/main/capi_internal.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/operator/string_cast.hpp"

using duckdb::const_data_ptr_t;
using duckdb::Date;
using duckdb::date_t;
using duckdb::dtime_t;
using duckdb::hugeint_t;
using duckdb::interval_t;
using duckdb::LogicalType;
using duckdb::string;
using duckdb::string_t;
using duckdb::Time;
using duckdb::Timestamp;
using duckdb::timestamp_t;
using duckdb::Value;
using duckdb::Vector;

namespace duckdb {

//===--------------------------------------------------------------------===//
// Unsafe Fetch (for internal use only)
//===--------------------------------------------------------------------===//
template <class T>
T UnsafeFetch(duckdb_result *result, idx_t col, idx_t row) {
	D_ASSERT(row < result->__deprecated_row_count);
	return ((T *)result->__deprecated_columns[col].__deprecated_data)[row];
}

//===--------------------------------------------------------------------===//
// Fetch Default Value
//===--------------------------------------------------------------------===//
struct FetchDefaultValue {
	template <class T>
	static T Operation() {
		return 0;
	}
};

template <>
date_t FetchDefaultValue::Operation() {
	date_t result;
	result.days = 0;
	return result;
}

template <>
dtime_t FetchDefaultValue::Operation() {
	dtime_t result;
	result.micros = 0;
	return result;
}

template <>
timestamp_t FetchDefaultValue::Operation() {
	timestamp_t result;
	result.value = 0;
	return result;
}

template <>
interval_t FetchDefaultValue::Operation() {
	interval_t result;
	result.months = 0;
	result.days = 0;
	result.micros = 0;
	return result;
}

template <>
char *FetchDefaultValue::Operation() {
	return nullptr;
}

template <>
duckdb_blob FetchDefaultValue::Operation() {
	duckdb_blob result;
	result.data = nullptr;
	result.size = 0;
	return result;
}

//===--------------------------------------------------------------------===//
// String Casts
//===--------------------------------------------------------------------===//
template <class OP>
struct FromCStringCastWrapper {
	template <class SOURCE_TYPE, class RESULT_TYPE>
	static bool Operation(SOURCE_TYPE input_str, RESULT_TYPE &result) {
		string_t input(input_str);
		return OP::template Operation<string_t, RESULT_TYPE>(input, result);
	}
};

template <class OP>
struct ToCStringCastWrapper {
	template <class SOURCE_TYPE, class RESULT_TYPE>
	static bool Operation(SOURCE_TYPE input, RESULT_TYPE &result) {
		Vector result_vector(LogicalType::VARCHAR, nullptr);
		auto result_string = OP::template Operation<SOURCE_TYPE>(input, result_vector);
		auto result_size = result_string.GetSize();
		auto result_data = result_string.GetDataUnsafe();

		result = (char *)duckdb_malloc(result_size + 1);
		memcpy(result, result_data, result_size);
		result[result_size] = '\0';
		return true;
	}
};

//===--------------------------------------------------------------------===//
// Blob Casts
//===--------------------------------------------------------------------===//
struct FromCBlobCastWrapper {
	template <class SOURCE_TYPE, class RESULT_TYPE>
	static bool Operation(SOURCE_TYPE input_str, RESULT_TYPE &result) {
		return false;
	}
};

template <>
bool FromCBlobCastWrapper::Operation(duckdb_blob input, char *&result) {
	string_t input_str((const char *)input.data, input.size);
	return ToCStringCastWrapper<duckdb::CastFromBlob>::template Operation<string_t, char *>(input_str, result);
}

} // namespace duckdb

using duckdb::FetchDefaultValue;
using duckdb::FromCBlobCastWrapper;
using duckdb::FromCStringCastWrapper;
using duckdb::ToCStringCastWrapper;
using duckdb::UnsafeFetch;

//===--------------------------------------------------------------------===//
// Templated Casts
//===--------------------------------------------------------------------===//
template <class SOURCE_TYPE, class RESULT_TYPE, class OP>
RESULT_TYPE TryCastCInternal(duckdb_result *result, idx_t col, idx_t row) {
	RESULT_TYPE result_value;
	try {
		if (!OP::template Operation<SOURCE_TYPE, RESULT_TYPE>(UnsafeFetch<SOURCE_TYPE>(result, col, row),
		                                                      result_value)) {
			return FetchDefaultValue::Operation<RESULT_TYPE>();
		}
	} catch (...) {
		return FetchDefaultValue::Operation<RESULT_TYPE>();
	}
	return result_value;
}

static bool CanUseDeprecatedFetch(duckdb_result *result, idx_t col, idx_t row) {
	if (!result) {
		return false;
	}
	if (!duckdb::deprecated_materialize_result(result)) {
		return false;
	}
	if (col >= result->__deprecated_column_count || row >= result->__deprecated_row_count) {
		return false;
	}
	return true;
}

static bool CanFetchValue(duckdb_result *result, idx_t col, idx_t row) {
	if (!CanUseDeprecatedFetch(result, col, row)) {
		return false;
	}
	if (result->__deprecated_columns[col].__deprecated_nullmask[row]) {
		return false;
	}
	return true;
}

template <class RESULT_TYPE, class OP = duckdb::TryCast>
static RESULT_TYPE GetInternalCValue(duckdb_result *result, idx_t col, idx_t row) {
	if (!CanFetchValue(result, col, row)) {
		return FetchDefaultValue::Operation<RESULT_TYPE>();
	}
	switch (result->__deprecated_columns[col].__deprecated_type) {
	case DUCKDB_TYPE_BOOLEAN:
		return TryCastCInternal<bool, RESULT_TYPE, OP>(result, col, row);
	case DUCKDB_TYPE_TINYINT:
		return TryCastCInternal<int8_t, RESULT_TYPE, OP>(result, col, row);
	case DUCKDB_TYPE_SMALLINT:
		return TryCastCInternal<int16_t, RESULT_TYPE, OP>(result, col, row);
	case DUCKDB_TYPE_INTEGER:
		return TryCastCInternal<int32_t, RESULT_TYPE, OP>(result, col, row);
	case DUCKDB_TYPE_BIGINT:
		return TryCastCInternal<int64_t, RESULT_TYPE, OP>(result, col, row);
	case DUCKDB_TYPE_UTINYINT:
		return TryCastCInternal<uint8_t, RESULT_TYPE, OP>(result, col, row);
	case DUCKDB_TYPE_USMALLINT:
		return TryCastCInternal<uint16_t, RESULT_TYPE, OP>(result, col, row);
	case DUCKDB_TYPE_UINTEGER:
		return TryCastCInternal<uint32_t, RESULT_TYPE, OP>(result, col, row);
	case DUCKDB_TYPE_UBIGINT:
		return TryCastCInternal<uint64_t, RESULT_TYPE, OP>(result, col, row);
	case DUCKDB_TYPE_FLOAT:
		return TryCastCInternal<float, RESULT_TYPE, OP>(result, col, row);
	case DUCKDB_TYPE_DOUBLE:
		return TryCastCInternal<double, RESULT_TYPE, OP>(result, col, row);
	case DUCKDB_TYPE_DATE:
		return TryCastCInternal<date_t, RESULT_TYPE, OP>(result, col, row);
	case DUCKDB_TYPE_TIME:
		return TryCastCInternal<dtime_t, RESULT_TYPE, OP>(result, col, row);
	case DUCKDB_TYPE_TIMESTAMP:
		return TryCastCInternal<timestamp_t, RESULT_TYPE, OP>(result, col, row);
	case DUCKDB_TYPE_HUGEINT:
		return TryCastCInternal<hugeint_t, RESULT_TYPE, OP>(result, col, row);
	case DUCKDB_TYPE_DECIMAL:
		return TryCastCInternal<hugeint_t, RESULT_TYPE, OP>(result, col, row);
	case DUCKDB_TYPE_INTERVAL:
		return TryCastCInternal<interval_t, RESULT_TYPE, OP>(result, col, row);
	case DUCKDB_TYPE_VARCHAR:
		return TryCastCInternal<char *, RESULT_TYPE, FromCStringCastWrapper<OP>>(result, col, row);
	case DUCKDB_TYPE_BLOB:
		return TryCastCInternal<duckdb_blob, RESULT_TYPE, FromCBlobCastWrapper>(result, col, row);
	default: // LCOV_EXCL_START
		// invalid type for C to C++ conversion
		D_ASSERT(0);
		return FetchDefaultValue::Operation<RESULT_TYPE>();
	} // LCOV_EXCL_STOP
}

//===--------------------------------------------------------------------===//
// duckdb_value_ functions
//===--------------------------------------------------------------------===//
bool duckdb_value_boolean(duckdb_result *result, idx_t col, idx_t row) {
	return GetInternalCValue<bool>(result, col, row);
}

int8_t duckdb_value_int8(duckdb_result *result, idx_t col, idx_t row) {
	return GetInternalCValue<int8_t>(result, col, row);
}

int16_t duckdb_value_int16(duckdb_result *result, idx_t col, idx_t row) {
	return GetInternalCValue<int16_t>(result, col, row);
}

int32_t duckdb_value_int32(duckdb_result *result, idx_t col, idx_t row) {
	return GetInternalCValue<int32_t>(result, col, row);
}

int64_t duckdb_value_int64(duckdb_result *result, idx_t col, idx_t row) {
	return GetInternalCValue<int64_t>(result, col, row);
}

duckdb_decimal duckdb_value_decimal(duckdb_result *result, idx_t col, idx_t row) {
	duckdb_decimal result_value;

	auto result_data = (duckdb::DuckDBResultData *)result->internal_data;
	result_data->result->types[col].GetDecimalProperties(result_value.width, result_value.scale);

	auto internal_value = GetInternalCValue<hugeint_t>(result, col, row);
	result_value.value.lower = internal_value.lower;
	result_value.value.upper = internal_value.upper;
	return result_value;
}

duckdb_hugeint duckdb_value_hugeint(duckdb_result *result, idx_t col, idx_t row) {
	duckdb_hugeint result_value;
	auto internal_value = GetInternalCValue<hugeint_t>(result, col, row);
	result_value.lower = internal_value.lower;
	result_value.upper = internal_value.upper;
	return result_value;
}

uint8_t duckdb_value_uint8(duckdb_result *result, idx_t col, idx_t row) {
	return GetInternalCValue<uint8_t>(result, col, row);
}

uint16_t duckdb_value_uint16(duckdb_result *result, idx_t col, idx_t row) {
	return GetInternalCValue<uint16_t>(result, col, row);
}

uint32_t duckdb_value_uint32(duckdb_result *result, idx_t col, idx_t row) {
	return GetInternalCValue<uint32_t>(result, col, row);
}

uint64_t duckdb_value_uint64(duckdb_result *result, idx_t col, idx_t row) {
	return GetInternalCValue<uint64_t>(result, col, row);
}

float duckdb_value_float(duckdb_result *result, idx_t col, idx_t row) {
	return GetInternalCValue<float>(result, col, row);
}

double duckdb_value_double(duckdb_result *result, idx_t col, idx_t row) {
	return GetInternalCValue<double>(result, col, row);
}

duckdb_date duckdb_value_date(duckdb_result *result, idx_t col, idx_t row) {
	duckdb_date result_value;
	result_value.days = GetInternalCValue<date_t>(result, col, row).days;
	return result_value;
}

duckdb_time duckdb_value_time(duckdb_result *result, idx_t col, idx_t row) {
	duckdb_time result_value;
	result_value.micros = GetInternalCValue<dtime_t>(result, col, row).micros;
	return result_value;
}

duckdb_timestamp duckdb_value_timestamp(duckdb_result *result, idx_t col, idx_t row) {
	duckdb_timestamp result_value;
	result_value.micros = GetInternalCValue<timestamp_t>(result, col, row).value;
	return result_value;
}

duckdb_interval duckdb_value_interval(duckdb_result *result, idx_t col, idx_t row) {
	duckdb_interval result_value;
	auto ival = GetInternalCValue<interval_t>(result, col, row);
	result_value.months = ival.months;
	result_value.days = ival.days;
	result_value.micros = ival.micros;
	return result_value;
}

char *duckdb_value_varchar(duckdb_result *result, idx_t col, idx_t row) {
	return GetInternalCValue<char *, ToCStringCastWrapper<duckdb::StringCast>>(result, col, row);
}

char *duckdb_value_varchar_internal(duckdb_result *result, idx_t col, idx_t row) {
	if (!CanFetchValue(result, col, row)) {
		return nullptr;
	}
	if (duckdb_column_type(result, col) != DUCKDB_TYPE_VARCHAR) {
		return nullptr;
	}
	return UnsafeFetch<char *>(result, col, row);
}

duckdb_blob duckdb_value_blob(duckdb_result *result, idx_t col, idx_t row) {
	if (CanFetchValue(result, col, row) && result->__deprecated_columns[col].__deprecated_type == DUCKDB_TYPE_BLOB) {
		auto internal_result = UnsafeFetch<duckdb_blob>(result, col, row);

		duckdb_blob result_blob;
		result_blob.data = malloc(internal_result.size);
		result_blob.size = internal_result.size;
		memcpy(result_blob.data, internal_result.data, internal_result.size);
		return result_blob;
	}
	return FetchDefaultValue::Operation<duckdb_blob>();
}

bool duckdb_value_is_null(duckdb_result *result, idx_t col, idx_t row) {
	if (!CanUseDeprecatedFetch(result, col, row)) {
		return false;
	}
	return result->__deprecated_columns[col].__deprecated_nullmask[row];
}
