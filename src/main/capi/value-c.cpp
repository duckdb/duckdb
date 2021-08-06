#include "duckdb/main/capi_internal.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/timestamp.hpp"

using duckdb::const_data_ptr_t;
using duckdb::Date;
using duckdb::date_t;
using duckdb::dtime_t;
using duckdb::hugeint_t;
using duckdb::interval_t;
using duckdb::string;
using duckdb::Time;
using duckdb::Timestamp;
using duckdb::timestamp_t;
using duckdb::Value;

template <class T>
T UnsafeFetch(duckdb_result *result, idx_t col, idx_t row) {
	D_ASSERT(row < result->row_count);
	return ((T *)result->columns[col].data)[row];
}

static Value GetCValue(duckdb_result *result, idx_t col, idx_t row) {
	if (col >= result->column_count) {
		return Value();
	}
	if (row >= result->row_count) {
		return Value();
	}
	if (result->columns[col].nullmask[row]) {
		return Value();
	}
	switch (result->columns[col].type) {
	case DUCKDB_TYPE_BOOLEAN:
		return Value::BOOLEAN(UnsafeFetch<bool>(result, col, row));
	case DUCKDB_TYPE_TINYINT:
		return Value::TINYINT(UnsafeFetch<int8_t>(result, col, row));
	case DUCKDB_TYPE_SMALLINT:
		return Value::SMALLINT(UnsafeFetch<int16_t>(result, col, row));
	case DUCKDB_TYPE_INTEGER:
		return Value::INTEGER(UnsafeFetch<int32_t>(result, col, row));
	case DUCKDB_TYPE_BIGINT:
		return Value::BIGINT(UnsafeFetch<int64_t>(result, col, row));
	case DUCKDB_TYPE_UTINYINT:
		return Value::TINYINT(UnsafeFetch<uint8_t>(result, col, row));
	case DUCKDB_TYPE_USMALLINT:
		return Value::SMALLINT(UnsafeFetch<uint16_t>(result, col, row));
	case DUCKDB_TYPE_UINTEGER:
		return Value::INTEGER(UnsafeFetch<uint32_t>(result, col, row));
	case DUCKDB_TYPE_UBIGINT:
		return Value::BIGINT(UnsafeFetch<uint64_t>(result, col, row));
	case DUCKDB_TYPE_FLOAT:
		return Value(UnsafeFetch<float>(result, col, row));
	case DUCKDB_TYPE_DOUBLE:
		return Value(UnsafeFetch<double>(result, col, row));
	case DUCKDB_TYPE_DATE: {
		auto date = UnsafeFetch<duckdb_date>(result, col, row);
		return Value::DATE(date_t(date.days));
	}
	case DUCKDB_TYPE_TIME: {
		auto time = UnsafeFetch<duckdb_time>(result, col, row);
		return Value::TIME(dtime_t(time.micros));
	}
	case DUCKDB_TYPE_TIMESTAMP_NS:
	case DUCKDB_TYPE_TIMESTAMP_MS:
	case DUCKDB_TYPE_TIMESTAMP_S:
	case DUCKDB_TYPE_TIMESTAMP: {
		auto timestamp = UnsafeFetch<duckdb_timestamp>(result, col, row);
		return Value::TIMESTAMP(timestamp_t(timestamp.micros));
	}
	case DUCKDB_TYPE_HUGEINT: {
		hugeint_t val;
		auto hugeint = UnsafeFetch<duckdb_hugeint>(result, col, row);
		val.lower = hugeint.lower;
		val.upper = hugeint.upper;
		return Value::HUGEINT(val);
	}
	case DUCKDB_TYPE_INTERVAL: {
		interval_t val;
		auto interval = UnsafeFetch<duckdb_interval>(result, col, row);
		val.days = interval.days;
		val.months = interval.months;
		val.micros = interval.micros;
		return Value::INTERVAL(val);
	}
	case DUCKDB_TYPE_VARCHAR:
		return Value(string(UnsafeFetch<const char *>(result, col, row)));
	case DUCKDB_TYPE_BLOB: {
		auto blob = UnsafeFetch<duckdb_blob>(result, col, row);
		return Value::BLOB((const_data_ptr_t)blob.data, blob.size);
	}
	default:
		// invalid type for C to C++ conversion
		D_ASSERT(0);
		return Value();
	}
}

const char *duckdb_column_name(duckdb_result *result, idx_t col) {
	if (!result || col >= result->column_count) {
		return nullptr;
	}
	return result->columns[col].name;
}

bool duckdb_value_boolean(duckdb_result *result, idx_t col, idx_t row) {
	Value val = GetCValue(result, col, row);
	if (val.is_null) {
		return false;
	} else {
		return val.GetValue<bool>();
	}
}

int8_t duckdb_value_int8(duckdb_result *result, idx_t col, idx_t row) {
	Value val = GetCValue(result, col, row);
	if (val.is_null) {
		return 0;
	} else {
		return val.GetValue<int8_t>();
	}
}

int16_t duckdb_value_int16(duckdb_result *result, idx_t col, idx_t row) {
	Value val = GetCValue(result, col, row);
	if (val.is_null) {
		return 0;
	} else {
		return val.GetValue<int16_t>();
	}
}

int32_t duckdb_value_int32(duckdb_result *result, idx_t col, idx_t row) {
	Value val = GetCValue(result, col, row);
	if (val.is_null) {
		return 0;
	} else {
		return val.GetValue<int32_t>();
	}
}

int64_t duckdb_value_int64(duckdb_result *result, idx_t col, idx_t row) {
	Value val = GetCValue(result, col, row);
	if (val.is_null) {
		return 0;
	} else {
		return val.GetValue<int64_t>();
	}
}

uint8_t duckdb_value_uint8(duckdb_result *result, idx_t col, idx_t row) {
	Value val = GetCValue(result, col, row);
	if (val.is_null) {
		return 0;
	} else {
		return val.GetValue<uint8_t>();
	}
}

uint16_t duckdb_value_uint16(duckdb_result *result, idx_t col, idx_t row) {
	Value val = GetCValue(result, col, row);
	if (val.is_null) {
		return 0;
	} else {
		return val.GetValue<uint16_t>();
	}
}

uint32_t duckdb_value_uint32(duckdb_result *result, idx_t col, idx_t row) {
	Value val = GetCValue(result, col, row);
	if (val.is_null) {
		return 0;
	} else {
		return val.GetValue<uint32_t>();
	}
}

uint64_t duckdb_value_uint64(duckdb_result *result, idx_t col, idx_t row) {
	Value val = GetCValue(result, col, row);
	if (val.is_null) {
		return 0;
	} else {
		return val.GetValue<uint64_t>();
	}
}

float duckdb_value_float(duckdb_result *result, idx_t col, idx_t row) {
	Value val = GetCValue(result, col, row);
	if (val.is_null) {
		return 0.0;
	} else {
		return val.GetValue<float>();
	}
}

double duckdb_value_double(duckdb_result *result, idx_t col, idx_t row) {
	Value val = GetCValue(result, col, row);
	if (val.is_null) {
		return 0.0;
	} else {
		return val.GetValue<double>();
	}
}

duckdb_date duckdb_value_date(duckdb_result *result, idx_t col, idx_t row) {
	Value val = GetCValue(result, col, row);
	duckdb_date date;
	if (val.is_null) {
		date.days = 0;
	} else {
		date.days = val.GetValue<date_t>().days;
	}
	return date;
}

duckdb_time duckdb_value_time(duckdb_result *result, idx_t col, idx_t row) {
	Value val = GetCValue(result, col, row);
	duckdb_time time;
	if (val.is_null) {
		time.micros = 0;
	} else {
		time.micros = val.GetValue<dtime_t>().micros;
	}
	return time;
}

duckdb_timestamp duckdb_value_timestamp(duckdb_result *result, idx_t col, idx_t row) {
	Value val = GetCValue(result, col, row);
	duckdb_timestamp ts;
	if (val.is_null) {
		ts.micros = 0;
	} else {
		ts.micros = val.GetValue<timestamp_t>().value;
	}
	return ts;
}

char *duckdb_value_varchar(duckdb_result *result, idx_t col, idx_t row) {
	Value val = GetCValue(result, col, row);
	return strdup(val.ToString().c_str());
}

duckdb_blob duckdb_value_blob(duckdb_result *result, idx_t col, idx_t row) {
	duckdb_blob blob;
	Value val = GetCValue(result, col, row).CastAs(LogicalType::BLOB);
	if (val.is_null) {
		blob.data = nullptr;
		blob.size = 0;
	} else {
		blob.data = malloc(val.str_value.size());
		memcpy((void *)blob.data, val.str_value.c_str(), val.str_value.size());
		blob.size = val.str_value.size();
	}
	return blob;
}
