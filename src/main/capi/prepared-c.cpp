#include "duckdb/main/capi/capi_internal.hpp"
#include "duckdb/main/query_result.hpp"
#include "duckdb/main/prepared_statement_data.hpp"
#include "duckdb/common/types/decimal.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/case_insensitive_map.hpp"

using duckdb::case_insensitive_map_t;
using duckdb::Connection;
using duckdb::date_t;
using duckdb::dtime_t;
using duckdb::ExtractStatementsWrapper;
using duckdb::hugeint_t;
using duckdb::LogicalType;
using duckdb::MaterializedQueryResult;
using duckdb::optional_ptr;
using duckdb::PreparedStatementWrapper;
using duckdb::QueryResultType;
using duckdb::StringUtil;
using duckdb::timestamp_t;
using duckdb::Value;

idx_t duckdb_extract_statements(duckdb_connection connection, const char *query,
                                duckdb_extracted_statements *out_extracted_statements) {
	if (!connection || !query || !out_extracted_statements) {
		return 0;
	}
	auto wrapper = new ExtractStatementsWrapper();
	Connection *conn = reinterpret_cast<Connection *>(connection);
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
	Connection *conn = reinterpret_cast<Connection *>(connection);
	auto source_wrapper = (ExtractStatementsWrapper *)extracted_statements;

	if (!connection || !out_prepared_statement || index >= source_wrapper->statements.size()) {
		return DuckDBError;
	}
	auto wrapper = new PreparedStatementWrapper();
	wrapper->statement = conn->Prepare(std::move(source_wrapper->statements[index]));

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
	Connection *conn = reinterpret_cast<Connection *>(connection);
	wrapper->statement = conn->Prepare(query);
	*out_prepared_statement = (duckdb_prepared_statement)wrapper;
	return !wrapper->statement->HasError() ? DuckDBSuccess : DuckDBError;
}

const char *duckdb_prepare_error(duckdb_prepared_statement prepared_statement) {
	auto wrapper = reinterpret_cast<PreparedStatementWrapper *>(prepared_statement);
	if (!wrapper || !wrapper->statement || !wrapper->statement->HasError()) {
		return nullptr;
	}
	return wrapper->statement->error.Message().c_str();
}

idx_t duckdb_nparams(duckdb_prepared_statement prepared_statement) {
	auto wrapper = reinterpret_cast<PreparedStatementWrapper *>(prepared_statement);
	if (!wrapper || !wrapper->statement || wrapper->statement->HasError()) {
		return 0;
	}
	return wrapper->statement->n_param;
}

static duckdb::string duckdb_parameter_name_internal(duckdb_prepared_statement prepared_statement, idx_t index) {
	auto wrapper = (PreparedStatementWrapper *)prepared_statement;
	if (!wrapper || !wrapper->statement || wrapper->statement->HasError()) {
		return duckdb::string();
	}
	if (index > wrapper->statement->n_param) {
		return duckdb::string();
	}
	for (auto &item : wrapper->statement->named_param_map) {
		auto &identifier = item.first;
		auto &param_idx = item.second;
		if (param_idx == index) {
			// Found the matching parameter
			return identifier;
		}
	}
	// No parameter was found with this index
	return duckdb::string();
}

const char *duckdb_parameter_name(duckdb_prepared_statement prepared_statement, idx_t index) {
	auto identifier = duckdb_parameter_name_internal(prepared_statement, index);
	if (identifier == duckdb::string()) {
		return NULL;
	}
	return strdup(identifier.c_str());
}

duckdb_type duckdb_param_type(duckdb_prepared_statement prepared_statement, idx_t param_idx) {
	auto wrapper = reinterpret_cast<PreparedStatementWrapper *>(prepared_statement);
	if (!wrapper || !wrapper->statement || wrapper->statement->HasError()) {
		return DUCKDB_TYPE_INVALID;
	}
	LogicalType param_type;
	auto identifier = std::to_string(param_idx);
	if (!wrapper->statement->data->TryGetType(identifier, param_type)) {
		return DUCKDB_TYPE_INVALID;
	}
	return ConvertCPPTypeToC(param_type);
}

duckdb_state duckdb_clear_bindings(duckdb_prepared_statement prepared_statement) {
	auto wrapper = reinterpret_cast<PreparedStatementWrapper *>(prepared_statement);
	if (!wrapper || !wrapper->statement || wrapper->statement->HasError()) {
		return DuckDBError;
	}
	wrapper->values.clear();
	return DuckDBSuccess;
}

duckdb_state duckdb_bind_value(duckdb_prepared_statement prepared_statement, idx_t param_idx, duckdb_value val) {
	auto value = reinterpret_cast<Value *>(val);
	auto wrapper = reinterpret_cast<PreparedStatementWrapper *>(prepared_statement);
	if (!wrapper || !wrapper->statement || wrapper->statement->HasError()) {
		return DuckDBError;
	}
	if (param_idx <= 0 || param_idx > wrapper->statement->n_param) {
		wrapper->statement->error =
		    duckdb::InvalidInputException("Can not bind to parameter number %d, statement only has %d parameter(s)",
		                                  param_idx, wrapper->statement->n_param);
		return DuckDBError;
	}
	auto identifier = duckdb_parameter_name_internal(prepared_statement, param_idx);
	wrapper->values[identifier] = *value;
	return DuckDBSuccess;
}

duckdb_state duckdb_bind_parameter_index(duckdb_prepared_statement prepared_statement, idx_t *param_idx_out,
                                         const char *name_p) {
	auto wrapper = (PreparedStatementWrapper *)prepared_statement;
	if (!wrapper || !wrapper->statement || wrapper->statement->HasError()) {
		return DuckDBError;
	}
	if (!name_p || !param_idx_out) {
		return DuckDBError;
	}
	auto name = std::string(name_p);
	for (auto &pair : wrapper->statement->named_param_map) {
		if (duckdb::StringUtil::CIEquals(pair.first, name)) {
			*param_idx_out = pair.second;
			return DuckDBSuccess;
		}
	}
	return DuckDBError;
}

duckdb_state duckdb_bind_boolean(duckdb_prepared_statement prepared_statement, idx_t param_idx, bool val) {
	auto value = Value::BOOLEAN(val);
	return duckdb_bind_value(prepared_statement, param_idx, (duckdb_value)&value);
}

duckdb_state duckdb_bind_int8(duckdb_prepared_statement prepared_statement, idx_t param_idx, int8_t val) {
	auto value = Value::TINYINT(val);
	return duckdb_bind_value(prepared_statement, param_idx, (duckdb_value)&value);
}

duckdb_state duckdb_bind_int16(duckdb_prepared_statement prepared_statement, idx_t param_idx, int16_t val) {
	auto value = Value::SMALLINT(val);
	return duckdb_bind_value(prepared_statement, param_idx, (duckdb_value)&value);
}

duckdb_state duckdb_bind_int32(duckdb_prepared_statement prepared_statement, idx_t param_idx, int32_t val) {
	auto value = Value::INTEGER(val);
	return duckdb_bind_value(prepared_statement, param_idx, (duckdb_value)&value);
}

duckdb_state duckdb_bind_int64(duckdb_prepared_statement prepared_statement, idx_t param_idx, int64_t val) {
	auto value = Value::BIGINT(val);
	return duckdb_bind_value(prepared_statement, param_idx, (duckdb_value)&value);
}

static hugeint_t duckdb_internal_hugeint(duckdb_hugeint val) {
	hugeint_t internal;
	internal.lower = val.lower;
	internal.upper = val.upper;
	return internal;
}

duckdb_state duckdb_bind_hugeint(duckdb_prepared_statement prepared_statement, idx_t param_idx, duckdb_hugeint val) {
	auto value = Value::HUGEINT(duckdb_internal_hugeint(val));
	return duckdb_bind_value(prepared_statement, param_idx, (duckdb_value)&value);
}

duckdb_state duckdb_bind_uint8(duckdb_prepared_statement prepared_statement, idx_t param_idx, uint8_t val) {
	auto value = Value::UTINYINT(val);
	return duckdb_bind_value(prepared_statement, param_idx, (duckdb_value)&value);
}

duckdb_state duckdb_bind_uint16(duckdb_prepared_statement prepared_statement, idx_t param_idx, uint16_t val) {
	auto value = Value::USMALLINT(val);
	return duckdb_bind_value(prepared_statement, param_idx, (duckdb_value)&value);
}

duckdb_state duckdb_bind_uint32(duckdb_prepared_statement prepared_statement, idx_t param_idx, uint32_t val) {
	auto value = Value::UINTEGER(val);
	return duckdb_bind_value(prepared_statement, param_idx, (duckdb_value)&value);
}

duckdb_state duckdb_bind_uint64(duckdb_prepared_statement prepared_statement, idx_t param_idx, uint64_t val) {
	auto value = Value::UBIGINT(val);
	return duckdb_bind_value(prepared_statement, param_idx, (duckdb_value)&value);
}

duckdb_state duckdb_bind_float(duckdb_prepared_statement prepared_statement, idx_t param_idx, float val) {
	auto value = Value::FLOAT(val);
	return duckdb_bind_value(prepared_statement, param_idx, (duckdb_value)&value);
}

duckdb_state duckdb_bind_double(duckdb_prepared_statement prepared_statement, idx_t param_idx, double val) {
	auto value = Value::DOUBLE(val);
	return duckdb_bind_value(prepared_statement, param_idx, (duckdb_value)&value);
}

duckdb_state duckdb_bind_date(duckdb_prepared_statement prepared_statement, idx_t param_idx, duckdb_date val) {
	auto value = Value::DATE(date_t(val.days));
	return duckdb_bind_value(prepared_statement, param_idx, (duckdb_value)&value);
}

duckdb_state duckdb_bind_time(duckdb_prepared_statement prepared_statement, idx_t param_idx, duckdb_time val) {
	auto value = Value::TIME(dtime_t(val.micros));
	return duckdb_bind_value(prepared_statement, param_idx, (duckdb_value)&value);
}

duckdb_state duckdb_bind_timestamp(duckdb_prepared_statement prepared_statement, idx_t param_idx,
                                   duckdb_timestamp val) {
	auto value = Value::TIMESTAMP(timestamp_t(val.micros));
	return duckdb_bind_value(prepared_statement, param_idx, (duckdb_value)&value);
}

duckdb_state duckdb_bind_interval(duckdb_prepared_statement prepared_statement, idx_t param_idx, duckdb_interval val) {
	auto value = Value::INTERVAL(val.months, val.days, val.micros);
	return duckdb_bind_value(prepared_statement, param_idx, (duckdb_value)&value);
}

duckdb_state duckdb_bind_varchar(duckdb_prepared_statement prepared_statement, idx_t param_idx, const char *val) {
	try {
		auto value = Value(val);
		return duckdb_bind_value(prepared_statement, param_idx, (duckdb_value)&value);
	} catch (...) {
		return DuckDBError;
	}
}

duckdb_state duckdb_bind_varchar_length(duckdb_prepared_statement prepared_statement, idx_t param_idx, const char *val,
                                        idx_t length) {
	try {
		auto value = Value(std::string(val, length));
		return duckdb_bind_value(prepared_statement, param_idx, (duckdb_value)&value);
	} catch (...) {
		return DuckDBError;
	}
}

duckdb_state duckdb_bind_decimal(duckdb_prepared_statement prepared_statement, idx_t param_idx, duckdb_decimal val) {
	auto hugeint_val = duckdb_internal_hugeint(val.value);
	if (val.width > duckdb::Decimal::MAX_WIDTH_INT64) {
		auto value = Value::DECIMAL(hugeint_val, val.width, val.scale);
		return duckdb_bind_value(prepared_statement, param_idx, (duckdb_value)&value);
	}
	auto value = hugeint_val.lower;
	auto duck_val = Value::DECIMAL((int64_t)value, val.width, val.scale);
	return duckdb_bind_value(prepared_statement, param_idx, (duckdb_value)&duck_val);
}

duckdb_state duckdb_bind_blob(duckdb_prepared_statement prepared_statement, idx_t param_idx, const void *data,
                              idx_t length) {
	auto value = Value::BLOB(duckdb::const_data_ptr_cast(data), length);
	return duckdb_bind_value(prepared_statement, param_idx, (duckdb_value)&value);
}

duckdb_state duckdb_bind_null(duckdb_prepared_statement prepared_statement, idx_t param_idx) {
	auto value = Value();
	return duckdb_bind_value(prepared_statement, param_idx, (duckdb_value)&value);
}

duckdb_state duckdb_execute_prepared(duckdb_prepared_statement prepared_statement, duckdb_result *out_result) {
	auto wrapper = reinterpret_cast<PreparedStatementWrapper *>(prepared_statement);
	if (!wrapper || !wrapper->statement || wrapper->statement->HasError()) {
		return DuckDBError;
	}

	auto result = wrapper->statement->Execute(wrapper->values, false);
	return duckdb_translate_result(std::move(result), out_result);
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
