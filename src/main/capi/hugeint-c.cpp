#include "duckdb/main/capi/capi_internal.hpp"
#include "duckdb/common/types/hugeint.hpp"
#include "duckdb/common/types/decimal.hpp"
#include "duckdb/common/operator/decimal_cast_operators.hpp"
#include "duckdb/main/capi/cast/utils.hpp"
#include "duckdb/main/capi/cast/to_decimal.hpp"

using duckdb::Hugeint;
using duckdb::hugeint_t;
using duckdb::Value;

double duckdb_hugeint_to_double(duckdb_hugeint val) {
	hugeint_t internal;
	internal.lower = val.lower;
	internal.upper = val.upper;
	return Hugeint::Cast<double>(internal);
}

static duckdb_decimal to_decimal_cast(double val, uint8_t width, uint8_t scale) {
	if (width > duckdb::Decimal::MAX_WIDTH_INT64) {
		return duckdb::TryCastToDecimalCInternal<double, duckdb::ToCDecimalCastWrapper<hugeint_t>>(val, width, scale);
	}
	if (width > duckdb::Decimal::MAX_WIDTH_INT32) {
		return duckdb::TryCastToDecimalCInternal<double, duckdb::ToCDecimalCastWrapper<int64_t>>(val, width, scale);
	}
	if (width > duckdb::Decimal::MAX_WIDTH_INT16) {
		return duckdb::TryCastToDecimalCInternal<double, duckdb::ToCDecimalCastWrapper<int32_t>>(val, width, scale);
	}
	return duckdb::TryCastToDecimalCInternal<double, duckdb::ToCDecimalCastWrapper<int16_t>>(val, width, scale);
}

duckdb_decimal duckdb_double_to_decimal(double val, uint8_t width, uint8_t scale) {
	if (scale > width || width > duckdb::Decimal::MAX_WIDTH_INT128) {
		return duckdb::FetchDefaultValue::Operation<duckdb_decimal>();
	}
	return to_decimal_cast(val, width, scale);
}

duckdb_hugeint duckdb_double_to_hugeint(double val) {
	hugeint_t internal_result;
	if (!Value::DoubleIsFinite(val) || !Hugeint::TryConvert<double>(val, internal_result)) {
		internal_result.lower = 0;
		internal_result.upper = 0;
	}

	duckdb_hugeint result;
	result.lower = internal_result.lower;
	result.upper = internal_result.upper;
	return result;
}

double duckdb_decimal_to_double(duckdb_decimal val) {
	double result;
	hugeint_t value;
	value.lower = val.value.lower;
	value.upper = val.value.upper;
	duckdb::TryCastFromDecimal::Operation<hugeint_t, double>(value, result, nullptr, val.width, val.scale);
	return result;
}
