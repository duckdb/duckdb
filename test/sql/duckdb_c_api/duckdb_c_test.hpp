
#pragma once

#include "duckdb.h"

#include <memory>

template <class T>
int64_t get_numeric(duckdb_column column, duckdb_oid_t index) {
	T *data = (T *)column.data;
	return (int64_t)data[index];
}

static int64_t get_numeric(duckdb_column column, duckdb_oid_t row) {
	switch (column.type) {
	case DUCKDB_TYPE_TINYINT:
		return get_numeric<int8_t>(column, row);
	case DUCKDB_TYPE_SMALLINT:
		return get_numeric<int16_t>(column, row);
	case DUCKDB_TYPE_INTEGER:
		return get_numeric<int32_t>(column, row);
	case DUCKDB_TYPE_BIGINT:
		return get_numeric<int64_t>(column, row);
	default:
		return -1;
	}
}

#define NULL_NUMERIC std::numeric_limits<int64_t>::min()

static bool CHECK_NUMERIC_COLUMN(duckdb_result result, duckdb_oid_t column,
                                 std::vector<int64_t> values) {
	if (result.column_count <= column) {
		// out of bounds
		return false;
	}
	auto &col = result.columns[column];
	if (col.type < DUCKDB_TYPE_TINYINT || col.type > DUCKDB_TYPE_BIGINT) {
		// not numeric type
		return false;
	}
	if (values.size() != col.count) {
		return false;
	}
	for (auto i = 0; i < values.size(); i++) {
		if (values[i] == NULL_NUMERIC) {
			if (!duckdb_value_is_null(col, i)) {
				return false;
			}
		} else {
			int64_t data_value = get_numeric(col, i);
			if (data_value != values[i]) {
				return false;
			}
		}
	}
	return true;
}

#define NULL_DECIMAL NAN

static bool CHECK_DECIMAL_COLUMN(duckdb_result result, duckdb_oid_t column,
                                 std::vector<double> values) {
	if (result.column_count <= column) {
		// out of bounds
		return false;
	}
	auto &col = result.columns[column];
	if (col.type < DUCKDB_TYPE_TINYINT || col.type > DUCKDB_TYPE_BIGINT) {
		// not numeric type
		return false;
	}
	if (values.size() != col.count) {
		return false;
	}
	for (auto i = 0; i < values.size(); i++) {
		if (isnan(values[i])) {
			if (!duckdb_value_is_null(col, i)) {
				return false;
			}
		} else {
			int64_t data_value = get_numeric(col, i);
			if (std::abs(data_value - values[i]) <= std::numeric_limits<double>::epsilon()) {
				return false;
			}
		}
	}
	return true;
}

static bool CHECK_NUMERIC(duckdb_result result, duckdb_oid_t row,
                          duckdb_oid_t column, int64_t value) {
	if (result.column_count <= column || result.row_count <= row) {
		// out of bounds
		return false;
	}
	auto &col = result.columns[column];
	if (col.type < DUCKDB_TYPE_TINYINT || col.type > DUCKDB_TYPE_BIGINT) {
		// not numeric type
		return false;
	}
	if (value == NULL_NUMERIC) {
		return duckdb_value_is_null(col, row);
	} else {
		int64_t data_value = get_numeric(col, row);
		return data_value == value;
	}
}

static bool CHECK_STRING(duckdb_result result, duckdb_oid_t row,
                         duckdb_oid_t column, std::string value) {
	if (result.column_count < column || result.row_count < row) {
		// out of bounds
		return false;
	}
	auto &col = result.columns[column];
	if (col.type != DUCKDB_TYPE_VARCHAR) {
		// not string type
		return false;
	}
	char **ptr = (char **)col.data;
	return std::string(ptr[row]) == value;
}
