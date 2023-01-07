#include "duckdb/main/capi/capi_internal.hpp"
#include "duckdb/main/query_result.hpp"
#include "duckdb/main/prepared_statement_data.hpp"
#include "duckdb/common/types/decimal.hpp"

using duckdb::Connection;
using duckdb::date_t;
using duckdb::dtime_t;
using duckdb::ExtractStatementsWrapper;
using duckdb::hugeint_t;
using duckdb::LogicalType;
using duckdb::MaterializedQueryResult;
using duckdb::PreparedStatementWrapper;
using duckdb::QueryResultType;
using duckdb::timestamp_t;
using duckdb::Value;

idx_t duckdb_extract_statements(duckdb_connection connection, const char *query,
                                duckdb_extracted_statements *out_extracted_statements) {
	if (!connection || !query || !out_extracted_statements) {
		return 0;
	}
	auto wrapper = new ExtractStatementsWrapper();
	Connection *conn = (Connection *)connection;
	try {
		wrapper->statements = conn->ExtractStatements(query);
	} catch (const duckdb::ParserException &e) {
		wrapper->error = e.what();
	}

	*out_extracted_statements = (duckdb_extracted_statements)wrapper;
	return wrapper->statements.size();
}

duckdb_state duckdb_prepare_extracted_statement(duckdb_connection connection,
                                                duckdb_extracted_statements extracted_statements, idx_t index,
                                                duckdb_prepared_statement *out_prepared_statement) {
	Connection *conn = (Connection *)connection;
	auto source_wrapper = (ExtractStatementsWrapper *)extracted_statements;

	if (!connection || !out_prepared_statement || index >= source_wrapper->statements.size()) {
		return DuckDBError;
	}
	auto wrapper = new PreparedStatementWrapper();
	wrapper->statement = conn->Prepare(move(source_wrapper->statements[index]));

	*out_prepared_statement = (duckdb_prepared_statement)wrapper;
	return wrapper->statement->HasError() ? DuckDBError : DuckDBSuccess;
}

const char *duckdb_extract_statements_error(duckdb_extracted_statements extracted_statements) {
	auto wrapper = (ExtractStatementsWrapper *)extracted_statements;
	if (!wrapper || wrapper->error.empty()) {
		return nullptr;
	}
	return wrapper->error.c_str();
}

duckdb_state duckdb_prepare(duckdb_connection connection, const char *query,
                            duckdb_prepared_statement *out_prepared_statement) {
	if (!connection || !query || !out_prepared_statement) {
		return DuckDBError;
	}
	auto wrapper = new PreparedStatementWrapper();
	Connection *conn = (Connection *)connection;
	wrapper->statement = conn->Prepare(query);
	*out_prepared_statement = (duckdb_prepared_statement)wrapper;
	return !wrapper->statement->HasError() ? DuckDBSuccess : DuckDBError;
}

const char *duckdb_prepare_error(duckdb_prepared_statement prepared_statement) {
	auto wrapper = (PreparedStatementWrapper *)prepared_statement;
	if (!wrapper || !wrapper->statement || !wrapper->statement->HasError()) {
		return nullptr;
	}
	return wrapper->statement->error.Message().c_str();
}

idx_t duckdb_nparams(duckdb_prepared_statement prepared_statement) {
	auto wrapper = (PreparedStatementWrapper *)prepared_statement;
	if (!wrapper || !wrapper->statement || wrapper->statement->HasError()) {
		return 0;
	}
	return wrapper->statement->n_param;
}

duckdb_type duckdb_param_type(duckdb_prepared_statement prepared_statement, idx_t param_idx) {
	auto wrapper = (PreparedStatementWrapper *)prepared_statement;
	if (!wrapper || !wrapper->statement || wrapper->statement->HasError()) {
		return DUCKDB_TYPE_INVALID;
	}
	LogicalType param_type;
	if (!wrapper->statement->data->TryGetType(param_idx, param_type)) {
		return DUCKDB_TYPE_INVALID;
	}
	return ConvertCPPTypeToC(param_type);
}

duckdb_state duckdb_clear_bindings(duckdb_prepared_statement prepared_statement) {
	auto wrapper = (PreparedStatementWrapper *)prepared_statement;
	if (!wrapper || !wrapper->statement || wrapper->statement->HasError()) {
		return DuckDBError;
	}
	wrapper->values.clear();
	return DuckDBSuccess;
}

static duckdb_state duckdb_bind_value(duckdb_prepared_statement prepared_statement, idx_t param_idx, Value val) {
	auto wrapper = (PreparedStatementWrapper *)prepared_statement;
	if (!wrapper || !wrapper->statement || wrapper->statement->HasError()) {
		return DuckDBError;
	}
	if (param_idx <= 0 || param_idx > wrapper->statement->n_param) {
		return DuckDBError;
	}
	if (param_idx > wrapper->values.size()) {
		wrapper->values.resize(param_idx);
	}
	wrapper->values[param_idx - 1] = val;
	return DuckDBSuccess;
}

duckdb_state duckdb_bind_boolean(duckdb_prepared_statement prepared_statement, idx_t param_idx, bool val) {
	return duckdb_bind_value(prepared_statement, param_idx, Value::BOOLEAN(val));
}

duckdb_state duckdb_bind_int8(duckdb_prepared_statement prepared_statement, idx_t param_idx, int8_t val) {
	return duckdb_bind_value(prepared_statement, param_idx, Value::TINYINT(val));
}

duckdb_state duckdb_bind_int16(duckdb_prepared_statement prepared_statement, idx_t param_idx, int16_t val) {
	return duckdb_bind_value(prepared_statement, param_idx, Value::SMALLINT(val));
}

duckdb_state duckdb_bind_int32(duckdb_prepared_statement prepared_statement, idx_t param_idx, int32_t val) {
	return duckdb_bind_value(prepared_statement, param_idx, Value::INTEGER(val));
}

duckdb_state duckdb_bind_int64(duckdb_prepared_statement prepared_statement, idx_t param_idx, int64_t val) {
	return duckdb_bind_value(prepared_statement, param_idx, Value::BIGINT(val));
}

static hugeint_t duckdb_internal_hugeint(duckdb_hugeint val) {
	hugeint_t internal;
	internal.lower = val.lower;
	internal.upper = val.upper;
	return internal;
}

duckdb_state duckdb_bind_hugeint(duckdb_prepared_statement prepared_statement, idx_t param_idx, duckdb_hugeint val) {
	return duckdb_bind_value(prepared_statement, param_idx, Value::HUGEINT(duckdb_internal_hugeint(val)));
}

duckdb_state duckdb_bind_uint8(duckdb_prepared_statement prepared_statement, idx_t param_idx, uint8_t val) {
	return duckdb_bind_value(prepared_statement, param_idx, Value::UTINYINT(val));
}

duckdb_state duckdb_bind_uint16(duckdb_prepared_statement prepared_statement, idx_t param_idx, uint16_t val) {
	return duckdb_bind_value(prepared_statement, param_idx, Value::USMALLINT(val));
}

duckdb_state duckdb_bind_uint32(duckdb_prepared_statement prepared_statement, idx_t param_idx, uint32_t val) {
	return duckdb_bind_value(prepared_statement, param_idx, Value::UINTEGER(val));
}

duckdb_state duckdb_bind_uint64(duckdb_prepared_statement prepared_statement, idx_t param_idx, uint64_t val) {
	return duckdb_bind_value(prepared_statement, param_idx, Value::UBIGINT(val));
}

duckdb_state duckdb_bind_float(duckdb_prepared_statement prepared_statement, idx_t param_idx, float val) {
	return duckdb_bind_value(prepared_statement, param_idx, Value::FLOAT(val));
}

duckdb_state duckdb_bind_double(duckdb_prepared_statement prepared_statement, idx_t param_idx, double val) {
	return duckdb_bind_value(prepared_statement, param_idx, Value::DOUBLE(val));
}

duckdb_state duckdb_bind_date(duckdb_prepared_statement prepared_statement, idx_t param_idx, duckdb_date val) {
	return duckdb_bind_value(prepared_statement, param_idx, Value::DATE(date_t(val.days)));
}

duckdb_state duckdb_bind_time(duckdb_prepared_statement prepared_statement, idx_t param_idx, duckdb_time val) {
	return duckdb_bind_value(prepared_statement, param_idx, Value::TIME(dtime_t(val.micros)));
}

duckdb_state duckdb_bind_timestamp(duckdb_prepared_statement prepared_statement, idx_t param_idx,
                                   duckdb_timestamp val) {
	return duckdb_bind_value(prepared_statement, param_idx, Value::TIMESTAMP(timestamp_t(val.micros)));
}

duckdb_state duckdb_bind_interval(duckdb_prepared_statement prepared_statement, idx_t param_idx, duckdb_interval val) {
	return duckdb_bind_value(prepared_statement, param_idx, Value::INTERVAL(val.months, val.days, val.micros));
}

duckdb_state duckdb_bind_varchar(duckdb_prepared_statement prepared_statement, idx_t param_idx, const char *val) {
	try {
		return duckdb_bind_value(prepared_statement, param_idx, Value(val));
	} catch (...) {
		return DuckDBError;
	}
}

duckdb_state duckdb_bind_varchar_length(duckdb_prepared_statement prepared_statement, idx_t param_idx, const char *val,
                                        idx_t length) {
	try {
		return duckdb_bind_value(prepared_statement, param_idx, Value(std::string(val, length)));
	} catch (...) {
		return DuckDBError;
	}
}

duckdb_state duckdb_bind_decimal(duckdb_prepared_statement prepared_statement, idx_t param_idx, duckdb_decimal val) {
	auto hugeint_val = duckdb_internal_hugeint(val.value);
	if (val.width > duckdb::Decimal::MAX_WIDTH_INT64) {
		return duckdb_bind_value(prepared_statement, param_idx, Value::DECIMAL(hugeint_val, val.width, val.scale));
	}
	auto value = hugeint_val.lower;
	return duckdb_bind_value(prepared_statement, param_idx, Value::DECIMAL((int64_t)value, val.width, val.scale));
}

duckdb_state duckdb_bind_blob(duckdb_prepared_statement prepared_statement, idx_t param_idx, const void *data,
                              idx_t length) {
	return duckdb_bind_value(prepared_statement, param_idx, Value::BLOB((duckdb::const_data_ptr_t)data, length));
}

duckdb_state duckdb_bind_null(duckdb_prepared_statement prepared_statement, idx_t param_idx) {
	return duckdb_bind_value(prepared_statement, param_idx, Value());
}

duckdb_state duckdb_execute_prepared(duckdb_prepared_statement prepared_statement, duckdb_result *out_result) {
	auto wrapper = (PreparedStatementWrapper *)prepared_statement;
	if (!wrapper || !wrapper->statement || wrapper->statement->HasError()) {
		return DuckDBError;
	}
	auto result = wrapper->statement->Execute(wrapper->values, false);
	return duckdb_translate_result(move(result), out_result);
}

template <class T>
void duckdb_destroy(void **wrapper) {
	if (!wrapper) {
		return;
	}

	auto casted = (T *)*wrapper;
	if (casted) {
		delete casted;
	}
	*wrapper = nullptr;
}

void duckdb_destroy_extracted(duckdb_extracted_statements *extracted_statements) {
	duckdb_destroy<ExtractStatementsWrapper>(reinterpret_cast<void **>(extracted_statements));
}

void duckdb_destroy_prepare(duckdb_prepared_statement *prepared_statement) {
	duckdb_destroy<PreparedStatementWrapper>(reinterpret_cast<void **>(prepared_statement));
}
