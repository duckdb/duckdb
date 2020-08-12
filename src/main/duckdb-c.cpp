#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb.h"
#include "duckdb.hpp"

#include <cstring>

#ifdef _WIN32
#define strdup _strdup
#endif

using namespace duckdb;

static LogicalType ConvertCTypeToCPP(duckdb_type type);
static duckdb_type ConvertCPPTypeToC(LogicalType type);
static idx_t GetCTypeSize(duckdb_type type);

struct DatabaseData {
	DatabaseData() : database(nullptr) {
	}
	~DatabaseData() {
		if (database) {
			delete database;
		}
	}

	DuckDB *database;
};

duckdb_state duckdb_open(const char *path, duckdb_database *out) {
	auto wrapper = new DatabaseData();
	try {
		wrapper->database = new DuckDB(path);
	} catch (...) {
		delete wrapper;
		return DuckDBError;
	}
	*out = (duckdb_database)wrapper;
	return DuckDBSuccess;
}

void duckdb_close(duckdb_database *database) {
	if (*database) {
		auto wrapper = (DatabaseData *)*database;
		delete wrapper;
		*database = nullptr;
	}
}

duckdb_state duckdb_connect(duckdb_database database, duckdb_connection *out) {
	auto wrapper = (DatabaseData *)database;
	Connection *connection;
	try {
		connection = new Connection(*wrapper->database);
	} catch (...) {
		return DuckDBError;
	}
	*out = (duckdb_connection)connection;
	return DuckDBSuccess;
}

void duckdb_disconnect(duckdb_connection *connection) {
	if (*connection) {
		Connection *conn = (Connection *)*connection;
		delete conn;
		*connection = nullptr;
	}
}

template <class T> void WriteData(duckdb_result *out, ChunkCollection &source, idx_t col) {
	idx_t row = 0;
	auto target = (T *)out->columns[col].data;
	for (auto &chunk : source.chunks) {
		auto source = FlatVector::GetData<T>(chunk->data[col]);
		for (idx_t k = 0; k < chunk->size(); k++) {
			target[row++] = source[k];
		}
	}
}

static duckdb_state duckdb_translate_result(MaterializedQueryResult *result, duckdb_result *out) {
	assert(result);
	if (!out) {
		// no result to write to, only return the status
		return result->success ? DuckDBSuccess : DuckDBError;
	}
	out->error_message = NULL;
	if (!result->success) {
		// write the error message
		out->error_message = strdup(result->error.c_str());
		return DuckDBError;
	}
	// copy the data
	// first write the meta data
	out->column_count = result->types.size();
	out->row_count = result->collection.count;
	out->columns = (duckdb_column *)malloc(sizeof(duckdb_column) * out->column_count);
	if (!out->columns) {
		return DuckDBError;
	}
	// zero initialize the columns (so we can cleanly delete it in case a malloc fails)
	memset(out->columns, 0, sizeof(duckdb_column) * out->column_count);
	for (idx_t i = 0; i < out->column_count; i++) {
		out->columns[i].type = ConvertCPPTypeToC(result->sql_types[i]);
		out->columns[i].name = strdup(result->names[i].c_str());
		out->columns[i].nullmask = (bool *)malloc(sizeof(bool) * out->row_count);
		out->columns[i].data = malloc(GetCTypeSize(out->columns[i].type) * out->row_count);
		if (!out->columns[i].nullmask || !out->columns[i].name || !out->columns[i].data) {
			// malloc failure
			return DuckDBError;
		}
		// memset data to 0 for VARCHAR columns for safe deletion later
		if (result->types[i] == TypeId::VARCHAR) {
			memset(out->columns[i].data, 0, GetCTypeSize(out->columns[i].type) * out->row_count);
		}
	}
	// now write the data
	for (idx_t col = 0; col < out->column_count; col++) {
		// first set the nullmask
		idx_t row = 0;
		for (auto &chunk : result->collection.chunks) {
			for (idx_t k = 0; k < chunk->size(); k++) {
				out->columns[col].nullmask[row++] = FlatVector::IsNull(chunk->data[col], k);
			}
		}
		// then write the data
		switch (result->sql_types[col].id) {
		case LogicalTypeId::BOOLEAN:
			WriteData<bool>(out, result->collection, col);
			break;
		case LogicalTypeId::TINYINT:
			WriteData<int8_t>(out, result->collection, col);
			break;
		case LogicalTypeId::SMALLINT:
			WriteData<int16_t>(out, result->collection, col);
			break;
		case LogicalTypeId::INTEGER:
			WriteData<int32_t>(out, result->collection, col);
			break;
		case LogicalTypeId::BIGINT:
			WriteData<int64_t>(out, result->collection, col);
			break;
		case LogicalTypeId::FLOAT:
			WriteData<float>(out, result->collection, col);
			break;
		case LogicalTypeId::DECIMAL:
		case LogicalTypeId::DOUBLE:
			WriteData<double>(out, result->collection, col);
			break;
		case LogicalTypeId::VARCHAR: {
			idx_t row = 0;
			auto target = (const char **)out->columns[col].data;
			for (auto &chunk : result->collection.chunks) {
				auto source = FlatVector::GetData<string_t>(chunk->data[col]);
				for (idx_t k = 0; k < chunk->size(); k++) {
					if (!FlatVector::IsNull(chunk->data[col], k)) {
						target[row] = strdup(source[k].GetData());
					}
					row++;
				}
			}
			break;
		}
		case LogicalTypeId::DATE: {
			idx_t row = 0;
			auto target = (duckdb_date *)out->columns[col].data;
			for (auto &chunk : result->collection.chunks) {
				auto source = FlatVector::GetData<date_t>(chunk->data[col]);
				for (idx_t k = 0; k < chunk->size(); k++) {
					if (!FlatVector::IsNull(chunk->data[col], k)) {
						int32_t year, month, day;
						Date::Convert(source[k], year, month, day);
						target[row].year = year;
						target[row].month = month;
						target[row].day = day;
					}
					row++;
				}
			}
			break;
		}
		case LogicalTypeId::TIME: {
			idx_t row = 0;
			auto target = (duckdb_time *)out->columns[col].data;
			for (auto &chunk : result->collection.chunks) {
				auto source = FlatVector::GetData<dtime_t>(chunk->data[col]);
				for (idx_t k = 0; k < chunk->size(); k++) {
					if (!FlatVector::IsNull(chunk->data[col], k)) {
						int32_t hour, min, sec, msec;
						Time::Convert(source[k], hour, min, sec, msec);
						target[row].hour = hour;
						target[row].min = min;
						target[row].sec = sec;
						target[row].msec = msec;
					}
					row++;
				}
			}
			break;
		}
		case LogicalTypeId::TIMESTAMP: {
			idx_t row = 0;
			auto target = (duckdb_timestamp *)out->columns[col].data;
			for (auto &chunk : result->collection.chunks) {
				auto source = FlatVector::GetData<timestamp_t>(chunk->data[col]);
				for (idx_t k = 0; k < chunk->size(); k++) {
					if (!FlatVector::IsNull(chunk->data[col], k)) {
						date_t date;
						dtime_t time;
						Timestamp::Convert(source[k], date, time);

						int32_t year, month, day;
						Date::Convert(date, year, month, day);

						int32_t hour, min, sec, msec;
						Time::Convert(time, hour, min, sec, msec);

						target[row].date.year = year;
						target[row].date.month = month;
						target[row].date.day = day;
						target[row].time.hour = hour;
						target[row].time.min = min;
						target[row].time.sec = sec;
						target[row].time.msec = msec;
					}
					row++;
				}
			}
			break;
		}
		case LogicalTypeId::HUGEINT: {
			idx_t row = 0;
			auto target = (duckdb_hugeint *)out->columns[col].data;
			for (auto &chunk : result->collection.chunks) {
				auto source = FlatVector::GetData<hugeint_t>(chunk->data[col]);
				for (idx_t k = 0; k < chunk->size(); k++) {
					if (!FlatVector::IsNull(chunk->data[col], k)) {
						target[row].lower = source[k].lower;
						target[row].upper = source[k].upper;
					}
					row++;
				}
			}
			break;
		}
		case LogicalTypeId::INTERVAL: {
			idx_t row = 0;
			auto target = (duckdb_interval *)out->columns[col].data;
			for (auto &chunk : result->collection.chunks) {
				auto source = FlatVector::GetData<interval_t>(chunk->data[col]);
				for (idx_t k = 0; k < chunk->size(); k++) {
					if (!FlatVector::IsNull(chunk->data[col], k)) {
						target[row].days = source[k].days;
						target[row].months = source[k].months;
						target[row].msecs = source[k].msecs;
					}
					row++;
				}
			}
			break;
		}
		default:
			// unsupported type for C API
			assert(0);
			return DuckDBError;
		}
	}
	return DuckDBSuccess;
}

duckdb_state duckdb_query(duckdb_connection connection, const char *query, duckdb_result *out) {
	Connection *conn = (Connection *)connection;
	auto result = conn->Query(query);
	return duckdb_translate_result(result.get(), out);
}

static void duckdb_destroy_column(duckdb_column column, idx_t count) {
	if (column.data) {
		if (column.type == DUCKDB_TYPE_VARCHAR) {
			// varchar, delete individual strings
			auto data = (char **)column.data;
			for (idx_t i = 0; i < count; i++) {
				if (data[i]) {
					free(data[i]);
				}
			}
		}
		free(column.data);
	}
	if (column.nullmask) {
		free(column.nullmask);
	}
	if (column.name) {
		free(column.name);
	}
}

void duckdb_destroy_result(duckdb_result *result) {
	if (result->error_message) {
		free(result->error_message);
	}
	if (result->columns) {
		for (idx_t i = 0; i < result->column_count; i++) {
			duckdb_destroy_column(result->columns[i], result->row_count);
		}
		free(result->columns);
	}
	memset(result, 0, sizeof(duckdb_result));
}

struct PreparedStatementWrapper {
	PreparedStatementWrapper() : statement(nullptr) {
	}
	~PreparedStatementWrapper() {
	}
	unique_ptr<PreparedStatement> statement;
	vector<Value> values;
};

duckdb_state duckdb_prepare(duckdb_connection connection, const char *query,
                            duckdb_prepared_statement *out_prepared_statement) {
	if (!connection || !query) {
		return DuckDBError;
	}
	auto wrapper = new PreparedStatementWrapper();
	Connection *conn = (Connection *)connection;
	wrapper->statement = conn->Prepare(query);
	*out_prepared_statement = (duckdb_prepared_statement)wrapper;
	return wrapper->statement->success ? DuckDBSuccess : DuckDBError;
}

duckdb_state duckdb_nparams(duckdb_prepared_statement prepared_statement, idx_t *nparams_out) {
	auto wrapper = (PreparedStatementWrapper *)prepared_statement;
	if (!wrapper || !wrapper->statement || !wrapper->statement->success || wrapper->statement->is_invalidated) {
		return DuckDBError;
	}
	*nparams_out = wrapper->statement->n_param;
	return DuckDBSuccess;
}

static duckdb_state duckdb_bind_value(duckdb_prepared_statement prepared_statement, idx_t param_idx, Value val) {
	auto wrapper = (PreparedStatementWrapper *)prepared_statement;
	if (!wrapper || !wrapper->statement || !wrapper->statement->success || wrapper->statement->is_invalidated) {
		return DuckDBError;
	}
	if (param_idx > wrapper->statement->n_param) {
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

duckdb_state duckdb_bind_float(duckdb_prepared_statement prepared_statement, idx_t param_idx, float val) {
	return duckdb_bind_value(prepared_statement, param_idx, Value(val));
}

duckdb_state duckdb_bind_double(duckdb_prepared_statement prepared_statement, idx_t param_idx, double val) {
	return duckdb_bind_value(prepared_statement, param_idx, Value(val));
}

duckdb_state duckdb_bind_varchar(duckdb_prepared_statement prepared_statement, idx_t param_idx, const char *val) {
	return duckdb_bind_value(prepared_statement, param_idx, Value(val));
}

duckdb_state duckdb_bind_null(duckdb_prepared_statement prepared_statement, idx_t param_idx) {
	return duckdb_bind_value(prepared_statement, param_idx, Value());
}

duckdb_state duckdb_execute_prepared(duckdb_prepared_statement prepared_statement, duckdb_result *out_result) {
	auto wrapper = (PreparedStatementWrapper *)prepared_statement;
	if (!wrapper || !wrapper->statement || !wrapper->statement->success || wrapper->statement->is_invalidated) {
		return DuckDBError;
	}
	auto result = wrapper->statement->Execute(wrapper->values, false);
	assert(result->type == QueryResultType::MATERIALIZED_RESULT);
	auto mat_res = (MaterializedQueryResult *)result.get();
	return duckdb_translate_result(mat_res, out_result);
}

void duckdb_destroy_prepare(duckdb_prepared_statement *prepared_statement) {
	if (!prepared_statement) {
		return;
	}
	auto wrapper = (PreparedStatementWrapper *)*prepared_statement;
	if (wrapper) {
		delete wrapper;
	}
	*prepared_statement = nullptr;
}

duckdb_type ConvertCPPTypeToC(LogicalType sql_type) {
	switch (sql_type.id) {
	case LogicalTypeId::BOOLEAN:
		return DUCKDB_TYPE_BOOLEAN;
	case LogicalTypeId::TINYINT:
		return DUCKDB_TYPE_TINYINT;
	case LogicalTypeId::SMALLINT:
		return DUCKDB_TYPE_SMALLINT;
	case LogicalTypeId::INTEGER:
		return DUCKDB_TYPE_INTEGER;
	case LogicalTypeId::BIGINT:
		return DUCKDB_TYPE_BIGINT;
	case LogicalTypeId::HUGEINT:
		return DUCKDB_TYPE_HUGEINT;
	case LogicalTypeId::FLOAT:
		return DUCKDB_TYPE_FLOAT;
	case LogicalTypeId::DECIMAL:
	case LogicalTypeId::DOUBLE:
		return DUCKDB_TYPE_DOUBLE;
	case LogicalTypeId::TIMESTAMP:
		return DUCKDB_TYPE_TIMESTAMP;
	case LogicalTypeId::DATE:
		return DUCKDB_TYPE_DATE;
	case LogicalTypeId::TIME:
		return DUCKDB_TYPE_TIME;
	case LogicalTypeId::VARCHAR:
		return DUCKDB_TYPE_VARCHAR;
	case LogicalTypeId::INTERVAL:
		return DUCKDB_TYPE_INTERVAL;
	default:
		return DUCKDB_TYPE_INVALID;
	}
}

LogicalType ConvertCTypeToCPP(duckdb_type type) {
	switch (type) {
	case DUCKDB_TYPE_BOOLEAN:
		return LogicalType(LogicalTypeId::BOOLEAN);
	case DUCKDB_TYPE_TINYINT:
		return LogicalType::TINYINT;
	case DUCKDB_TYPE_SMALLINT:
		return LogicalType::SMALLINT;
	case DUCKDB_TYPE_INTEGER:
		return LogicalType::INTEGER;
	case DUCKDB_TYPE_BIGINT:
		return LogicalType::BIGINT;
	case DUCKDB_TYPE_HUGEINT:
		return LogicalType::HUGEINT;
	case DUCKDB_TYPE_FLOAT:
		return LogicalType::FLOAT;
	case DUCKDB_TYPE_DOUBLE:
		return LogicalType::DOUBLE;
	case DUCKDB_TYPE_TIMESTAMP:
		return LogicalType::TIMESTAMP;
	case DUCKDB_TYPE_DATE:
		return LogicalType::DATE;
	case DUCKDB_TYPE_TIME:
		return LogicalType::TIME;
	case DUCKDB_TYPE_VARCHAR:
		return LogicalType::VARCHAR;
	case DUCKDB_TYPE_INTERVAL:
		return LogicalType::INTERVAL;
	default:
		return LogicalType(LogicalTypeId::INVALID);
	}
}

idx_t GetCTypeSize(duckdb_type type) {
	switch (type) {
	case DUCKDB_TYPE_BOOLEAN:
		return sizeof(bool);
	case DUCKDB_TYPE_TINYINT:
		return sizeof(int8_t);
	case DUCKDB_TYPE_SMALLINT:
		return sizeof(int16_t);
	case DUCKDB_TYPE_INTEGER:
		return sizeof(int32_t);
	case DUCKDB_TYPE_BIGINT:
		return sizeof(int64_t);
	case DUCKDB_TYPE_HUGEINT:
		return sizeof(duckdb_hugeint);
	case DUCKDB_TYPE_FLOAT:
		return sizeof(float);
	case DUCKDB_TYPE_DOUBLE:
		return sizeof(double);
	case DUCKDB_TYPE_DATE:
		return sizeof(duckdb_date);
	case DUCKDB_TYPE_TIME:
		return sizeof(duckdb_time);
	case DUCKDB_TYPE_TIMESTAMP:
		return sizeof(duckdb_timestamp);
	case DUCKDB_TYPE_VARCHAR:
		return sizeof(const char *);
	case DUCKDB_TYPE_INTERVAL:
		return sizeof(duckdb_interval);
	default:
		// unsupported type
		assert(0);
		return sizeof(const char *);
	}
}

template <class T> T UnsafeFetch(duckdb_result *result, idx_t col, idx_t row) {
	assert(row < result->row_count);
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
	case DUCKDB_TYPE_FLOAT:
		return Value(UnsafeFetch<float>(result, col, row));
	case DUCKDB_TYPE_DOUBLE:
		return Value(UnsafeFetch<double>(result, col, row));
	case DUCKDB_TYPE_DATE: {
		auto date = UnsafeFetch<duckdb_date>(result, col, row);
		return Value::DATE(date.year, date.month, date.day);
	}
	case DUCKDB_TYPE_TIME: {
		auto time = UnsafeFetch<duckdb_time>(result, col, row);
		return Value::TIME(time.hour, time.min, time.sec, time.msec);
	}
	case DUCKDB_TYPE_TIMESTAMP: {
		auto timestamp = UnsafeFetch<duckdb_timestamp>(result, col, row);
		return Value::TIMESTAMP(timestamp.date.year, timestamp.date.month, timestamp.date.day, timestamp.time.hour,
		                        timestamp.time.min, timestamp.time.sec, timestamp.time.msec);
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
		val.msecs = interval.msecs;
		return Value::INTERVAL(val);
	}
	case DUCKDB_TYPE_VARCHAR:
		return Value(string(UnsafeFetch<const char *>(result, col, row)));
	default:
		// invalid type for C to C++ conversion
		assert(0);
		return Value();
	}
}

bool duckdb_value_boolean(duckdb_result *result, idx_t col, idx_t row) {
	Value val = GetCValue(result, col, row);
	if (val.is_null) {
		return false;
	} else {
		return val.CastAs(TypeId::BOOL).value_.boolean;
	}
}

int8_t duckdb_value_int8(duckdb_result *result, idx_t col, idx_t row) {
	Value val = GetCValue(result, col, row);
	if (val.is_null) {
		return 0;
	} else {
		return val.CastAs(TypeId::INT8).value_.tinyint;
	}
}

int16_t duckdb_value_int16(duckdb_result *result, idx_t col, idx_t row) {
	Value val = GetCValue(result, col, row);
	if (val.is_null) {
		return 0;
	} else {
		return val.CastAs(TypeId::INT16).value_.smallint;
	}
}

int32_t duckdb_value_int32(duckdb_result *result, idx_t col, idx_t row) {
	Value val = GetCValue(result, col, row);
	if (val.is_null) {
		return 0;
	} else {
		return val.CastAs(TypeId::INT32).value_.integer;
	}
}

int64_t duckdb_value_int64(duckdb_result *result, idx_t col, idx_t row) {
	Value val = GetCValue(result, col, row);
	if (val.is_null) {
		return 0;
	} else {
		return val.CastAs(TypeId::INT64).value_.bigint;
	}
}

float duckdb_value_float(duckdb_result *result, idx_t col, idx_t row) {
	Value val = GetCValue(result, col, row);
	if (val.is_null) {
		return 0.0;
	} else {
		return val.CastAs(TypeId::FLOAT).value_.float_;
	}
}

double duckdb_value_double(duckdb_result *result, idx_t col, idx_t row) {
	Value val = GetCValue(result, col, row);
	if (val.is_null) {
		return 0.0;
	} else {
		return val.CastAs(TypeId::DOUBLE).value_.double_;
	}
}

char *duckdb_value_varchar(duckdb_result *result, idx_t col, idx_t row) {
	Value val = GetCValue(result, col, row);
	return strdup(val.ToString(ConvertCTypeToCPP(result->columns[col].type)).c_str());
}
