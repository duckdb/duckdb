#include "duckdb/main/capi/capi_internal.hpp"
#include "duckdb/main/capi/cast/generic.hpp"
#include "duckdb/common/types/decimal.hpp"
#include "duckdb/main/capi/cast/utils.hpp"
#include "duckdb/main/capi/cast/to_decimal.hpp"

namespace duckdb {

template <class OP>
struct FromCStringToCDecimalCastWrapper {
	template <class SOURCE_TYPE>
	static bool Operation(SOURCE_TYPE input_str, duckdb_decimal &result, std::string *error, uint8_t width,
	                      uint8_t scale) {
		string_t input(input_str);
		return OP::template Operation<string_t>(input, result, error, width, scale);
	}
};

struct FromCBlobToCDecimalCastWrapper {
	template <class SOURCE_TYPE>
	static bool Operation(SOURCE_TYPE input_str, duckdb_decimal &result, std::string *error, uint8_t width,
	                      uint8_t scale) {
		//! Only blob -> string cast is supported, we know we're only doing ToDecimal here, so we don't need any
		//! specialization.
		return false;
	}
};

} // namespace duckdb

using duckdb::date_t;
using duckdb::dtime_t;
using duckdb::FetchDefaultValue;
using duckdb::hugeint_t;
using duckdb::interval_t;
using duckdb::StringCast;
using duckdb::timestamp_t;
using duckdb::ToCStringCastWrapper;
using duckdb::TryCastToDecimalCInternal;
using duckdb::UnsafeFetch;

template <class INTERNAL_TYPE>
duckdb_decimal GetInternalAsDecimalCValue(duckdb_result *result, idx_t col, idx_t row, uint8_t width, uint8_t scale) {
	using OP = duckdb::ToCDecimalCastWrapper<INTERNAL_TYPE>;
	if (!CanFetchValue(result, col, row)) {
		return duckdb::FetchDefaultValue::Operation<duckdb_decimal>();
	}
	switch (result->__deprecated_columns[col].__deprecated_type) {
	case DUCKDB_TYPE_BOOLEAN:
		return TryCastToDecimalCInternal<bool, OP>(result, col, row, width, scale);
	case DUCKDB_TYPE_TINYINT:
		return TryCastToDecimalCInternal<int8_t, OP>(result, col, row, width, scale);
	case DUCKDB_TYPE_SMALLINT:
		return TryCastToDecimalCInternal<int16_t, OP>(result, col, row, width, scale);
	case DUCKDB_TYPE_INTEGER:
		return TryCastToDecimalCInternal<int32_t, OP>(result, col, row, width, scale);
	case DUCKDB_TYPE_BIGINT:
		return TryCastToDecimalCInternal<int64_t, OP>(result, col, row, width, scale);
	case DUCKDB_TYPE_UTINYINT:
		return TryCastToDecimalCInternal<uint8_t, OP>(result, col, row, width, scale);
	case DUCKDB_TYPE_USMALLINT:
		return TryCastToDecimalCInternal<uint16_t, OP>(result, col, row, width, scale);
	case DUCKDB_TYPE_UINTEGER:
		return TryCastToDecimalCInternal<uint32_t, OP>(result, col, row, width, scale);
	case DUCKDB_TYPE_UBIGINT:
		return TryCastToDecimalCInternal<uint64_t, OP>(result, col, row, width, scale);
	case DUCKDB_TYPE_FLOAT:
		return TryCastToDecimalCInternal<float, OP>(result, col, row, width, scale);
	case DUCKDB_TYPE_DOUBLE:
		return TryCastToDecimalCInternal<double, OP>(result, col, row, width, scale);
	case DUCKDB_TYPE_DATE:
		return TryCastToDecimalCInternal<date_t, OP>(result, col, row, width, scale);
	case DUCKDB_TYPE_TIME:
		return TryCastToDecimalCInternal<dtime_t, OP>(result, col, row, width, scale);
	case DUCKDB_TYPE_TIMESTAMP:
		return TryCastToDecimalCInternal<timestamp_t, OP>(result, col, row, width, scale);
	case DUCKDB_TYPE_HUGEINT:
		return TryCastToDecimalCInternal<hugeint_t, OP>(result, col, row, width, scale);
	case DUCKDB_TYPE_DECIMAL:
		//! TODO add Decimal -> Decimal conversion
		return FetchDefaultValue::Operation<duckdb_decimal>();
		// return TryCastToDecimalCInternal<hugeint_t,
	case DUCKDB_TYPE_INTERVAL:
		return TryCastToDecimalCInternal<interval_t, OP>(result, col, row, width, scale);
	case DUCKDB_TYPE_VARCHAR:
		return TryCastToDecimalCInternal<char *, duckdb::FromCStringToCDecimalCastWrapper<OP>>(result, col, row, width,
		                                                                                       scale);
	case DUCKDB_TYPE_BLOB:
		return TryCastToDecimalCInternal<duckdb_blob, duckdb::FromCBlobToCDecimalCastWrapper>(result, col, row, width,
		                                                                                      scale);
	default: // LCOV_EXCL_START
		// invalid type for C to C++ conversion
		D_ASSERT(0);
		return FetchDefaultValue::Operation<duckdb_decimal>();
	} // LCOV_EXCL_STOP
}

duckdb_decimal GetInternalAsDecimalCastSwitch(duckdb_result *result, idx_t col, idx_t row, uint8_t width,
                                              uint8_t scale) {
	if (scale > width || width > duckdb::DecimalType::MaxWidth()) {
		//! Invalid format
		return FetchDefaultValue::Operation<duckdb_decimal>();
	}
	if (width > duckdb::Decimal::MAX_WIDTH_INT64) {
		return GetInternalAsDecimalCValue<hugeint_t>(result, col, row, width, scale);
	}
	if (width > duckdb::Decimal::MAX_WIDTH_INT32) {
		return GetInternalAsDecimalCValue<int64_t>(result, col, row, width, scale);
	}
	if (width > duckdb::Decimal::MAX_WIDTH_INT16) {
		return GetInternalAsDecimalCValue<int32_t>(result, col, row, width, scale);
	}
	return GetInternalAsDecimalCValue<int16_t>(result, col, row, width, scale);
}
