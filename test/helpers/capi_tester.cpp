#include "capi_tester.hpp"

bool NO_FAIL(duckdb::CAPIResult &result) {
	return result.success;
}

bool NO_FAIL(duckdb::unique_ptr<duckdb::CAPIResult> result) {
	return NO_FAIL(*result);
}

namespace duckdb {

template <>
bool CAPIResult::Fetch(idx_t col, idx_t row) {
	return duckdb_value_boolean(&result, col, row);
}

template <>
int8_t CAPIResult::Fetch(idx_t col, idx_t row) {
	return duckdb_value_int8(&result, col, row);
}

template <>
int16_t CAPIResult::Fetch(idx_t col, idx_t row) {
	return duckdb_value_int16(&result, col, row);
}

template <>
int32_t CAPIResult::Fetch(idx_t col, idx_t row) {
	return duckdb_value_int32(&result, col, row);
}

template <>
int64_t CAPIResult::Fetch(idx_t col, idx_t row) {
	return duckdb_value_int64(&result, col, row);
}

template <>
uint8_t CAPIResult::Fetch(idx_t col, idx_t row) {
	return duckdb_value_uint8(&result, col, row);
}

template <>
uint16_t CAPIResult::Fetch(idx_t col, idx_t row) {
	return duckdb_value_uint16(&result, col, row);
}

template <>
uint32_t CAPIResult::Fetch(idx_t col, idx_t row) {
	return duckdb_value_uint32(&result, col, row);
}

template <>
uint64_t CAPIResult::Fetch(idx_t col, idx_t row) {
	return duckdb_value_uint64(&result, col, row);
}

template <>
float CAPIResult::Fetch(idx_t col, idx_t row) {
	return duckdb_value_float(&result, col, row);
}

template <>
double CAPIResult::Fetch(idx_t col, idx_t row) {
	return duckdb_value_double(&result, col, row);
}

template <>
duckdb_decimal CAPIResult::Fetch(idx_t col, idx_t row) {
	return duckdb_value_decimal(&result, col, row);
}

template <>
duckdb_date CAPIResult::Fetch(idx_t col, idx_t row) {
	auto data = (duckdb_date *)duckdb_column_data(&result, col);
	return data[row];
}

template <>
duckdb_time CAPIResult::Fetch(idx_t col, idx_t row) {
	auto data = (duckdb_time *)duckdb_column_data(&result, col);
	return data[row];
}

template <>
duckdb_timestamp CAPIResult::Fetch(idx_t col, idx_t row) {
	auto data = (duckdb_timestamp *)duckdb_column_data(&result, col);
	return data[row];
}

template <>
duckdb_interval CAPIResult::Fetch(idx_t col, idx_t row) {
	return duckdb_value_interval(&result, col, row);
}

template <>
duckdb_blob CAPIResult::Fetch(idx_t col, idx_t row) {
	auto data = (duckdb_blob *)duckdb_column_data(&result, col);
	return data[row];
}

template <>
string CAPIResult::Fetch(idx_t col, idx_t row) {
	auto value = duckdb_value_varchar(&result, col, row);
	string strval = value ? string(value) : string();
	free((void *)value);
	return strval;
}

template <>
duckdb_date_struct CAPIResult::Fetch(idx_t col, idx_t row) {
	auto value = duckdb_value_date(&result, col, row);
	return duckdb_from_date(value);
}

template <>
duckdb_time_struct CAPIResult::Fetch(idx_t col, idx_t row) {
	auto value = duckdb_value_time(&result, col, row);
	return duckdb_from_time(value);
}

template <>
duckdb_timestamp_struct CAPIResult::Fetch(idx_t col, idx_t row) {
	auto value = duckdb_value_timestamp(&result, col, row);
	return duckdb_from_timestamp(value);
}

template <>
duckdb_hugeint CAPIResult::Fetch(idx_t col, idx_t row) {
	return duckdb_value_hugeint(&result, col, row);
}

template <>
duckdb_uhugeint CAPIResult::Fetch(idx_t col, idx_t row) {
	return duckdb_value_uhugeint(&result, col, row);
}

} // namespace duckdb
