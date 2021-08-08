#include "duckdb/main/capi_internal.hpp"
#include "duckdb/common/types/hugeint.hpp"

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
	if (!Value::DoubleIsValid(val) || !Hugeint::TryConvert<double>(val, internal_result)) {
		internal_result.lower = 0;
		internal_result.upper = 0;
	}

	duckdb_hugeint result;
	result.lower = internal_result.lower;
	result.upper = internal_result.upper;
	return result;
}
