#include "duckdb/main/capi_internal.hpp"
#include "duckdb/common/types/hugeint.hpp"
#include "duckdb/common/types/decimal.hpp"
#include "duckdb/common/operator/decimal_cast_operators.hpp"

using duckdb::Hugeint;
using duckdb::hugeint_t;
using duckdb::Value;

double duckdb_hugeint_to_double(duckdb_hugeint val) {
	hugeint_t internal;
	internal.lower = val.lower;
	internal.upper = val.upper;
	return Hugeint::Cast<double>(internal);
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
