#include "common/vector_operations/vector_operations.hpp"
#include "duckdb.h"
#include "duckdb.hpp"

#include <cstring>

using namespace duckdb;

static duckdb_type ConvertCPPTypeToC(TypeId type);
static size_t GetCTypeSize(duckdb_type type);

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

template <class T> void WriteData(duckdb_result *out, ChunkCollection &source, size_t col) {
	size_t row = 0;
	T *target = (T *)out->columns[col].data;
	for (auto &chunk : source.chunks) {
		T *source = (T *)chunk->data[col].data;
		for (size_t k = 0; k < chunk->data[col].count; k++) {
			target[row++] = source[k];
		}
	}
}

duckdb_state duckdb_query(duckdb_connection connection, const char *query, duckdb_result *out) {
	Connection *conn = (Connection *)connection;
	auto result = conn->Query(query);
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
	for (size_t i = 0; i < out->column_count; i++) {
		out->columns[i].type = ConvertCPPTypeToC(result->types[i]);
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
	for (size_t col = 0; col < out->column_count; col++) {
		// first set the nullmask
		size_t row = 0;
		for (auto &chunk : result->collection.chunks) {
			assert(!chunk->data[col].sel_vector);
			for (size_t k = 0; k < chunk->data[col].count; k++) {
				out->columns[col].nullmask[row++] = chunk->data[col].nullmask[k];
			}
		}
		// then write the data
		switch (result->types[col]) {
		case TypeId::BOOLEAN:
			WriteData<bool>(out, result->collection, col);
			break;
		case TypeId::TINYINT:
			WriteData<int8_t>(out, result->collection, col);
			break;
		case TypeId::SMALLINT:
			WriteData<int16_t>(out, result->collection, col);
			break;
		case TypeId::INTEGER:
			WriteData<int32_t>(out, result->collection, col);
			break;
		case TypeId::BIGINT:
			WriteData<int64_t>(out, result->collection, col);
			break;
		case TypeId::DECIMAL:
			WriteData<double>(out, result->collection, col);
			break;
		case TypeId::POINTER:
			WriteData<uint64_t>(out, result->collection, col);
			break;
		case TypeId::VARCHAR: {
			size_t row = 0;
			const char **target = (const char **)out->columns[col].data;
			for (auto &chunk : result->collection.chunks) {
				const char **source = (const char **)chunk->data[col].data;
				for (size_t k = 0; k < chunk->data[col].count; k++) {
					if (!chunk->data[col].nullmask[k]) {
						target[row++] = strdup(source[k]);
					}
				}
			}
			break;
		}
		case TypeId::DATE: {
			size_t row = 0;
			duckdb_date *target = (duckdb_date *)out->columns[col].data;
			for (auto &chunk : result->collection.chunks) {
				date_t *source = (date_t *)chunk->data[col].data;
				for (size_t k = 0; k < chunk->data[col].count; k++) {
					if (chunk->data[col].nullmask[k]) {
						int32_t year, month, day;
						Date::Convert(source[k], year, month, day);
						target[row].year = year;
						target[row].month = month;
						target[row].day = day;
						row++;
					}
				}
			}
			break;
		}
		case TypeId::TIMESTAMP:
		default:
			// unsupported type for C API
			assert(0);
			return DuckDBError;
		}
	}
	return DuckDBSuccess;
}

static void duckdb_destroy_column(duckdb_column column, size_t count) {
	if (column.data) {
		if (column.type == DUCKDB_TYPE_VARCHAR) {
			// varchar, delete individual strings
			auto data = (char **)column.data;
			for (size_t i = 0; i < count; i++) {
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
		for (size_t i = 0; i < result->column_count; i++) {
			duckdb_destroy_column(result->columns[i], result->row_count);
		}
		free(result->columns);
	}
	memset(result, 0, sizeof(duckdb_result));
}

duckdb_type ConvertCPPTypeToC(TypeId type) {
	switch (type) {
	case TypeId::BOOLEAN:
		return DUCKDB_TYPE_BOOLEAN;
	case TypeId::TINYINT:
		return DUCKDB_TYPE_TINYINT;
	case TypeId::SMALLINT:
		return DUCKDB_TYPE_SMALLINT;
	case TypeId::INTEGER:
		return DUCKDB_TYPE_INTEGER;
	case TypeId::BIGINT:
		return DUCKDB_TYPE_BIGINT;
	case TypeId::DECIMAL:
		return DUCKDB_TYPE_DECIMAL;
	case TypeId::POINTER:
		return DUCKDB_TYPE_POINTER;
	case TypeId::TIMESTAMP:
		return DUCKDB_TYPE_TIMESTAMP;
	case TypeId::DATE:
		return DUCKDB_TYPE_DATE;
	case TypeId::VARCHAR:
		return DUCKDB_TYPE_VARCHAR;
	default:
		return DUCKDB_TYPE_INVALID;
	}
}

size_t GetCTypeSize(duckdb_type type) {
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
	case DUCKDB_TYPE_DECIMAL:
		return sizeof(double);
	case DUCKDB_TYPE_POINTER:
		return sizeof(uint64_t);
	case DUCKDB_TYPE_DATE:
		return sizeof(duckdb_date);
	case DUCKDB_TYPE_VARCHAR:
		return sizeof(const char *);
	default:
		// unsupported type
		assert(0);
		return sizeof(const char *);
	}
}

template <class T> T UnsafeFetch(duckdb_result *result, uint32_t col, uint64_t row) {
	assert(row < result->row_count);
	return ((T *)result->columns[col].data)[row];
}

static Value GetCValue(duckdb_result *result, uint32_t col, uint64_t row) {
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
	case DUCKDB_TYPE_DECIMAL:
		return Value(UnsafeFetch<double>(result, col, row));
	case DUCKDB_TYPE_POINTER:
		return Value::POINTER(UnsafeFetch<uint64_t>(result, col, row));
	case DUCKDB_TYPE_DATE: {
		auto date = UnsafeFetch<duckdb_date>(result, col, row);
		return Value::DATE(Date::FromDate(date.year, date.month, date.day));
	}
	case DUCKDB_TYPE_VARCHAR:
		return Value(string(UnsafeFetch<const char *>(result, col, row)));
	case DUCKDB_TYPE_TIMESTAMP:
	default:
		// invalid type for C to C++ conversion
		assert(0);
		return Value();
	}
}

int32_t duckdb_value_int32(duckdb_result *result, uint32_t col, uint64_t row) {
	Value val = GetCValue(result, col, row);
	if (val.is_null) {
		return 0;
	} else {
		return val.CastAs(TypeId::INTEGER).value_.integer;
	}
}

int64_t duckdb_value_int64(duckdb_result *result, uint32_t col, uint64_t row) {
	Value val = GetCValue(result, col, row);
	if (val.is_null) {
		return 0;
	} else {
		return val.CastAs(TypeId::BIGINT).value_.bigint;
	}
}

char *duckdb_value_varchar(duckdb_result *result, uint32_t col, uint64_t row) {
	Value val = GetCValue(result, col, row);
	if (val.is_null) {
		return strdup("NULL");
	} else {
		return strdup(val.ToString().c_str());
	}
}

int32_t duckdb_value_int32_unsafe(duckdb_result *result, uint32_t col, uint64_t row) {
	assert(col < result->column_count);
	assert(result->columns[col].type == DUCKDB_TYPE_INTEGER);
	return UnsafeFetch<int32_t>(result, col, row);
}

int64_t duckdb_value_int64_unsafe(duckdb_result *result, uint32_t col, uint64_t row) {
	assert(col < result->column_count);
	assert(result->columns[col].type == DUCKDB_TYPE_BIGINT);
	return UnsafeFetch<int64_t>(result, col, row);
}

const char *duckdb_value_varchar_unsafe(duckdb_result *result, uint32_t col, uint64_t row) {
	assert(col < result->column_count);
	assert(result->columns[col].type == DUCKDB_TYPE_VARCHAR);
	return UnsafeFetch<const char *>(result, col, row);
}
