#include "duckdb/main/capi/capi_internal.hpp"
#include "duckdb/common/uhugeint.hpp"

using duckdb::Appender;
using duckdb::AppenderWrapper;
using duckdb::Connection;
using duckdb::date_t;
using duckdb::dtime_t;
using duckdb::ErrorData;
using duckdb::hugeint_t;
using duckdb::interval_t;
using duckdb::string_t;
using duckdb::timestamp_t;
using duckdb::uhugeint_t;

duckdb_state duckdb_appender_create(duckdb_connection connection, const char *schema, const char *table,
                                    duckdb_appender *out_appender) {
	Connection *conn = reinterpret_cast<Connection *>(connection);

	if (!connection || !table || !out_appender) {
		return DuckDBError;
	}
	if (schema == nullptr) {
		schema = DEFAULT_SCHEMA;
	}
	auto wrapper = new AppenderWrapper();
	*out_appender = (duckdb_appender)wrapper;
	try {
		wrapper->appender = duckdb::make_uniq<Appender>(*conn, schema, table);
	} catch (std::exception &ex) {
		ErrorData error(ex);
		wrapper->error = error.RawMessage();
		return DuckDBError;
	} catch (...) { // LCOV_EXCL_START
		wrapper->error = "Unknown create appender error";
		return DuckDBError;
	} // LCOV_EXCL_STOP
	return DuckDBSuccess;
}

duckdb_state duckdb_appender_destroy(duckdb_appender *appender) {
	if (!appender || !*appender) {
		return DuckDBError;
	}
	auto state = duckdb_appender_close(*appender);
	auto wrapper = reinterpret_cast<AppenderWrapper *>(*appender);
	if (wrapper) {
		delete wrapper;
	}
	*appender = nullptr;
	return state;
}

template <class FUN>
duckdb_state duckdb_appender_run_function(duckdb_appender appender, FUN &&function) {
	if (!appender) {
		return DuckDBError;
	}
	auto wrapper = reinterpret_cast<AppenderWrapper *>(appender);
	if (!wrapper->appender) {
		return DuckDBError;
	}
	try {
		function(*wrapper->appender);
	} catch (std::exception &ex) {
		ErrorData error(ex);
		wrapper->error = error.RawMessage();
		return DuckDBError;
	} catch (...) { // LCOV_EXCL_START
		wrapper->error = "Unknown appender error.";
		return DuckDBError;
	} // LCOV_EXCL_STOP
	return DuckDBSuccess;
}

const char *duckdb_appender_error(duckdb_appender appender) {
	if (!appender) {
		return nullptr;
	}
	auto wrapper = reinterpret_cast<AppenderWrapper *>(appender);
	if (wrapper->error.empty()) {
		return nullptr;
	}
	return wrapper->error.c_str();
}

duckdb_state duckdb_appender_begin_row(duckdb_appender appender) {
	return DuckDBSuccess;
}

duckdb_state duckdb_appender_end_row(duckdb_appender appender) {
	return duckdb_appender_run_function(appender, [&](Appender &appender) { appender.EndRow(); });
}

template <class T>
duckdb_state duckdb_append_internal(duckdb_appender appender, T value) {
	if (!appender) {
		return DuckDBError;
	}
	auto *appender_instance = reinterpret_cast<AppenderWrapper *>(appender);
	try {
		appender_instance->appender->Append<T>(value);
	} catch (std::exception &ex) {
		ErrorData error(ex);
		appender_instance->error = error.RawMessage();
		return DuckDBError;
	} catch (...) {
		return DuckDBError;
	}
	return DuckDBSuccess;
}

duckdb_state duckdb_append_default(duckdb_appender appender) {
	if (!appender) {
		return DuckDBError;
	}
	auto *appender_instance = reinterpret_cast<AppenderWrapper *>(appender);

	try {
		appender_instance->appender->AppendDefault();
	} catch (std::exception &ex) {
		ErrorData error(ex);
		appender_instance->error = error.RawMessage();
		return DuckDBError;
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

duckdb_state duckdb_append_hugeint(duckdb_appender appender, duckdb_hugeint value) {
	hugeint_t internal;
	internal.lower = value.lower;
	internal.upper = value.upper;
	return duckdb_append_internal<hugeint_t>(appender, internal);
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

duckdb_state duckdb_append_uhugeint(duckdb_appender appender, duckdb_uhugeint value) {
	uhugeint_t internal;
	internal.lower = value.lower;
	internal.upper = value.upper;
	return duckdb_append_internal<uhugeint_t>(appender, internal);
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

duckdb_state duckdb_append_interval(duckdb_appender appender, duckdb_interval value) {
	interval_t interval;
	interval.months = value.months;
	interval.days = value.days;
	interval.micros = value.micros;
	return duckdb_append_internal<interval_t>(appender, interval);
}

duckdb_state duckdb_append_null(duckdb_appender appender) {
	return duckdb_append_internal<std::nullptr_t>(appender, nullptr);
}

duckdb_state duckdb_append_varchar(duckdb_appender appender, const char *val) {
	return duckdb_append_internal<const char *>(appender, val);
}

duckdb_state duckdb_append_varchar_length(duckdb_appender appender, const char *val, idx_t length) {
	return duckdb_append_internal<string_t>(appender, string_t(val, duckdb::UnsafeNumericCast<uint32_t>(length)));
}

duckdb_state duckdb_append_blob(duckdb_appender appender, const void *data, idx_t length) {
	auto value = duckdb::Value::BLOB((duckdb::const_data_ptr_t)data, length);
	return duckdb_append_internal<duckdb::Value>(appender, value);
}

duckdb_state duckdb_appender_flush(duckdb_appender appender) {
	return duckdb_appender_run_function(appender, [&](Appender &appender) { appender.Flush(); });
}

duckdb_state duckdb_appender_close(duckdb_appender appender) {
	return duckdb_appender_run_function(appender, [&](Appender &appender) { appender.Close(); });
}

idx_t duckdb_appender_column_count(duckdb_appender appender) {
	if (!appender) {
		return 0;
	}

	auto wrapper = reinterpret_cast<AppenderWrapper *>(appender);
	if (!wrapper->appender) {
		return 0;
	}

	return wrapper->appender->GetTypes().size();
}

duckdb_logical_type duckdb_appender_column_type(duckdb_appender appender, idx_t col_idx) {
	if (!appender || col_idx >= duckdb_appender_column_count(appender)) {
		return nullptr;
	}

	auto wrapper = reinterpret_cast<AppenderWrapper *>(appender);
	if (!wrapper->appender) {
		return nullptr;
	}

	return reinterpret_cast<duckdb_logical_type>(new duckdb::LogicalType(wrapper->appender->GetTypes()[col_idx]));
}

duckdb_state duckdb_append_data_chunk(duckdb_appender appender, duckdb_data_chunk chunk) {
	if (!chunk) {
		return DuckDBError;
	}
	auto data_chunk = (duckdb::DataChunk *)chunk;
	return duckdb_appender_run_function(appender, [&](Appender &appender) { appender.AppendDataChunk(*data_chunk); });
}
