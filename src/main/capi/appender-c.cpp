#include "duckdb/main/capi_internal.hpp"
#include "duckdb/main/appender.hpp"

using duckdb::Connection;
using duckdb::Appender;
using duckdb::string_t;
using duckdb::date_t;
using duckdb::dtime_t;
using duckdb::timestamp_t;

duckdb_state duckdb_appender_create(duckdb_connection connection, const char *schema, const char *table,
                                    duckdb_appender *out_appender) {
	Connection *conn = (Connection *)connection;

	if (!connection || !table || !out_appender) {
		return DuckDBError;
	}
	if (!schema) {
		schema = DEFAULT_SCHEMA;
	}
	try {
		auto *appender = new Appender(*conn, schema, table);
		*out_appender = appender;
	} catch (...) {
		return DuckDBError;
	}
	return DuckDBSuccess;
}

duckdb_state duckdb_appender_destroy(duckdb_appender *appender) {
	if (!appender || !*appender) {
		return DuckDBError;
	}
	auto *appender_instance = *((Appender **)appender);
	delete appender_instance;
	*appender = nullptr;
	return DuckDBSuccess;
}

template <class FUN>
duckdb_state duckdb_appender_run_function(duckdb_appender appender, FUN &&function) {
	if (!appender) {
		return DuckDBError;
	}
	auto *appender_instance = (Appender *)appender;
	try {
		function(*appender_instance);
	} catch (...) {
		return DuckDBError;
	}
	return DuckDBSuccess;
}

duckdb_state duckdb_appender_begin_row(duckdb_appender appender) {
	return duckdb_appender_run_function(appender, [&](Appender &appender) { appender.BeginRow(); });
}

duckdb_state duckdb_appender_end_row(duckdb_appender appender) {
	return duckdb_appender_run_function(appender, [&](Appender &appender) { appender.EndRow(); });
}

template <class T>
duckdb_state duckdb_append_internal(duckdb_appender appender, T value) {
	if (!appender) {
		return DuckDBError;
	}
	auto *appender_instance = (Appender *)appender;
	try {
		appender_instance->Append<T>(value);
	} catch (...) {
		return DuckDBError;
	}
	return DuckDBSuccess;
}

duckdb_state duckdb_append_bool(duckdb_appender appender, bool value) {
	return duckdb_append_internal<bool>(appender, value);
}

duckdb_state duckdb_append_int8(duckdb_appender appender, int8_t value) {
	return duckdb_append_internal<int8_t>(appender, value);
}

duckdb_state duckdb_append_int16(duckdb_appender appender, int16_t value) {
	return duckdb_append_internal<int16_t>(appender, value);
}

duckdb_state duckdb_append_int32(duckdb_appender appender, int32_t value) {
	return duckdb_append_internal<int32_t>(appender, value);
}

duckdb_state duckdb_append_int64(duckdb_appender appender, int64_t value) {
	return duckdb_append_internal<int64_t>(appender, value);
}

duckdb_state duckdb_append_uint8(duckdb_appender appender, uint8_t value) {
	return duckdb_append_internal<uint8_t>(appender, value);
}

duckdb_state duckdb_append_uint16(duckdb_appender appender, uint16_t value) {
	return duckdb_append_internal<uint16_t>(appender, value);
}

duckdb_state duckdb_append_uint32(duckdb_appender appender, uint32_t value) {
	return duckdb_append_internal<uint32_t>(appender, value);
}

duckdb_state duckdb_append_uint64(duckdb_appender appender, uint64_t value) {
	return duckdb_append_internal<uint64_t>(appender, value);
}

duckdb_state duckdb_append_float(duckdb_appender appender, float value) {
	return duckdb_append_internal<float>(appender, value);
}

duckdb_state duckdb_append_double(duckdb_appender appender, double value) {
	return duckdb_append_internal<double>(appender, value);
}

duckdb_state duckdb_append_date(duckdb_appender appender, duckdb_date value) {
	return duckdb_append_internal<date_t>(appender, date_t(value.days));
}

duckdb_state duckdb_append_time(duckdb_appender appender, duckdb_time value) {
	return duckdb_append_internal<dtime_t>(appender, dtime_t(value.micros));
}

duckdb_state duckdb_append_timestamp(duckdb_appender appender, duckdb_timestamp value) {
	return duckdb_append_internal<timestamp_t>(appender, timestamp_t(value.micros));
}

duckdb_state duckdb_append_null(duckdb_appender appender) {
	return duckdb_append_internal<std::nullptr_t>(appender, nullptr);
}

duckdb_state duckdb_append_varchar(duckdb_appender appender, const char *val) {
	return duckdb_append_internal<const char *>(appender, val);
}

duckdb_state duckdb_append_varchar_length(duckdb_appender appender, const char *val, idx_t length) {
	return duckdb_append_internal<string_t>(appender, string_t(val, length));
}
duckdb_state duckdb_append_blob(duckdb_appender appender, const void *data, idx_t length) {
	return duckdb_append_internal<string_t>(appender, string_t((const char *)data, length));
}

duckdb_state duckdb_appender_flush(duckdb_appender appender) {
	return duckdb_appender_run_function(appender, [&](Appender &appender) { appender.Flush(); });
}

duckdb_state duckdb_appender_close(duckdb_appender appender) {
	return duckdb_appender_run_function(appender, [&](Appender &appender) { appender.Close(); });
}
