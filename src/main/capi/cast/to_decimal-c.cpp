#include "duckdb/main/capi/capi_internal.hpp"
#include "duckdb/main/capi/cast/generic.hpp"

// duckdb_decimal GetInternalAsDecimalCValue(duckdb_result *result, idx_t col, idx_t row, uint8_t width, uint8_t scale)
// { 	if (!CanFetchValue(result, col, row)) { 		return FetchDefaultValue::Operation<duckdb_decimal>();
//	}
//	switch (result->__deprecated_columns[col].__deprecated_type) {
//	case DUCKDB_TYPE_BOOLEAN:
//		return TryCastCInternal<bool, RESULT_TYPE, OP>(result, col, row);
//	case DUCKDB_TYPE_TINYINT:
//		return TryCastCInternal<int8_t, RESULT_TYPE, OP>(result, col, row);
//	case DUCKDB_TYPE_SMALLINT:
//		return TryCastCInternal<int16_t, RESULT_TYPE, OP>(result, col, row);
//	case DUCKDB_TYPE_INTEGER:
//		return TryCastCInternal<int32_t, RESULT_TYPE, OP>(result, col, row);
//	case DUCKDB_TYPE_BIGINT:
//		return TryCastCInternal<int64_t, RESULT_TYPE, OP>(result, col, row);
//	case DUCKDB_TYPE_UTINYINT:
//		return TryCastCInternal<uint8_t, RESULT_TYPE, OP>(result, col, row);
//	case DUCKDB_TYPE_USMALLINT:
//		return TryCastCInternal<uint16_t, RESULT_TYPE, OP>(result, col, row);
//	case DUCKDB_TYPE_UINTEGER:
//		return TryCastCInternal<uint32_t, RESULT_TYPE, OP>(result, col, row);
//	case DUCKDB_TYPE_UBIGINT:
//		return TryCastCInternal<uint64_t, RESULT_TYPE, OP>(result, col, row);
//	case DUCKDB_TYPE_FLOAT:
//		return TryCastCInternal<float, RESULT_TYPE, OP>(result, col, row);
//	case DUCKDB_TYPE_DOUBLE:
//		return TryCastCInternal<double, RESULT_TYPE, OP>(result, col, row);
//	case DUCKDB_TYPE_DATE:
//		return TryCastCInternal<date_t, RESULT_TYPE, OP>(result, col, row);
//	case DUCKDB_TYPE_TIME:
//		return TryCastCInternal<dtime_t, RESULT_TYPE, OP>(result, col, row);
//	case DUCKDB_TYPE_TIMESTAMP:
//		return TryCastCInternal<timestamp_t, RESULT_TYPE, OP>(result, col, row);
//	case DUCKDB_TYPE_HUGEINT:
//		return TryCastCInternal<hugeint_t, RESULT_TYPE, OP>(result, col, row);
//	case DUCKDB_TYPE_DECIMAL:
//		return TryCastDecimalCInternal<RESULT_TYPE>(result, col, row);
//	case DUCKDB_TYPE_INTERVAL:
//		return TryCastCInternal<interval_t, RESULT_TYPE, OP>(result, col, row);
//	case DUCKDB_TYPE_VARCHAR:
//		return TryCastCInternal<char *, RESULT_TYPE, FromCStringCastWrapper<OP>>(result, col, row);
//	case DUCKDB_TYPE_BLOB:
//		return TryCastCInternal<duckdb_blob, RESULT_TYPE, FromCBlobCastWrapper>(result, col, row);
//	default: // LCOV_EXCL_START
//		// invalid type for C to C++ conversion
//		D_ASSERT(0);
//		return FetchDefaultValue::Operation<RESULT_TYPE>();
//	} // LCOV_EXCL_STOP
// }

namespace duckdb {

duckdb_decimal GetInternalAsDecimalCastSwitch(duckdb_result *result, idx_t col, idx_t row, uint8_t width,
                                              uint8_t scale) {
	return FetchDefaultValue::Operation<duckdb_decimal>();
}

} // namespace duckdb
