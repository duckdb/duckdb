#include "common/vector_operations/vector_operations.hpp"
#include "duckdb.h"
#include "duckdb.hpp"

#include <cstring>

using namespace duckdb;

duckdb_state duckdb_open(const char *path, duckdb_database *out) {
	DuckDB *database = new DuckDB(path);
	*out = (duckdb_database)database;
	return DuckDBSuccess;
}

duckdb_state duckdb_close(duckdb_database database) {
	if (database) {
		DuckDB *db = (DuckDB *)database;
		delete db;
	}
	return DuckDBSuccess;
}

duckdb_state duckdb_connect(duckdb_database database, duckdb_connection *out) {
	DuckDB *db = (DuckDB *)database;
	Connection *connection = new Connection(*db);
	*out = (duckdb_connection)connection;
	return DuckDBSuccess;
}

duckdb_state duckdb_disconnect(duckdb_connection connection) {
	if (connection) {
		Connection *conn = (Connection *)connection;
		delete conn;
	}
	return DuckDBSuccess;
}

static duckdb_type _convert_type_cpp_to_c(TypeId type) {
	switch (type) {
	case TypeId::PARAMETER_OFFSET:
		return DUCKDB_TYPE_PARAMETER_OFFSET;
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
	case TypeId::VARBINARY:
		return DUCKDB_TYPE_VARBINARY;
	case TypeId::ARRAY:
		return DUCKDB_TYPE_ARRAY;
	case TypeId::UDT:
		return DUCKDB_TYPE_UDT;
	default:
		return DUCKDB_TYPE_INVALID;
	}
}

static TypeId _convert_type_c_to_cpp(duckdb_type type) {
	switch (type) {
	case DUCKDB_TYPE_PARAMETER_OFFSET:
		return TypeId::PARAMETER_OFFSET;
	case DUCKDB_TYPE_BOOLEAN:
		return TypeId::BOOLEAN;
	case DUCKDB_TYPE_TINYINT:
		return TypeId::TINYINT;
	case DUCKDB_TYPE_SMALLINT:
		return TypeId::SMALLINT;
	case DUCKDB_TYPE_INTEGER:
		return TypeId::INTEGER;
	case DUCKDB_TYPE_BIGINT:
		return TypeId::BIGINT;
	case DUCKDB_TYPE_DECIMAL:
		return TypeId::DECIMAL;
	case DUCKDB_TYPE_POINTER:
		return TypeId::POINTER;
	case DUCKDB_TYPE_TIMESTAMP:
		return TypeId::TIMESTAMP;
	case DUCKDB_TYPE_DATE:
		return TypeId::DATE;
	case DUCKDB_TYPE_VARCHAR:
		return TypeId::VARCHAR;
	case DUCKDB_TYPE_VARBINARY:
		return TypeId::VARBINARY;
	case DUCKDB_TYPE_ARRAY:
		return TypeId::ARRAY;
	case DUCKDB_TYPE_UDT:
		return TypeId::UDT;
	default:
		return TypeId::INVALID;
	}
}

template <class T> T get_value(duckdb_column column, size_t index) {
	T *data = (T *)column.data;
	return data[index];
}

static Value _duckdb_c_get_value(duckdb_column column, size_t index) {
	switch (column.type) {
	case DUCKDB_TYPE_BOOLEAN:
		return Value::BOOLEAN(get_value<int8_t>(column, index));
	case DUCKDB_TYPE_TINYINT:
		return Value::TINYINT(get_value<int8_t>(column, index));
	case DUCKDB_TYPE_SMALLINT:
		return Value::SMALLINT(get_value<int16_t>(column, index));
	case DUCKDB_TYPE_INTEGER:
		return Value::INTEGER(get_value<int32_t>(column, index));
	case DUCKDB_TYPE_BIGINT:
		return Value::BIGINT(get_value<int64_t>(column, index));
	case DUCKDB_TYPE_DECIMAL:
		return Value(get_value<double>(column, index));
	case DUCKDB_TYPE_POINTER:
		return Value::POINTER(get_value<uint64_t>(column, index));
	case DUCKDB_TYPE_DATE:
		return Value::DATE(get_value<date_t>(column, index));
	case DUCKDB_TYPE_TIMESTAMP:
		return Value::TIMESTAMP(get_value<timestamp_t>(column, index));
	case DUCKDB_TYPE_VARCHAR:
		return Value(string(get_value<char *>(column, index)));
	default:
		throw std::runtime_error("Invalid value for C to C++ conversion!");
	}
}

int duckdb_value_is_null(duckdb_column column, size_t index) {
	return column.nullmask[index];
}

const char *duckdb_get_value_str(duckdb_column column, size_t index) {
	Value v = _duckdb_c_get_value(column, index);
	string str = v.ToString();
	char *cstr = new char[str.length() + 1];
	std::strcpy(cstr, str.c_str());
	return cstr;
}

void duckdb_print_result(duckdb_result result) {
	// print the result
	// first print the header
	for (size_t i = 0; i < result.column_count; i++) {
		printf("%s\t", TypeIdToString(_convert_type_c_to_cpp(result.columns[i].type)).c_str());
	}
	printf(" [ %zu ]\n", result.row_count);
	for (size_t j = 0; j < result.row_count; j++) {
		for (size_t i = 0; i < result.column_count; i++) {
			if (duckdb_value_is_null(result.columns[i], j)) {
				printf("NULL\t");
			} else {
				Value v = _duckdb_c_get_value(result.columns[i], j);
				printf("%s\t", v.ToString().c_str());
			}
		}
		printf("\n");
	}
	printf("\n");
}

duckdb_state duckdb_query(duckdb_connection connection, const char *query, duckdb_result *out) {
	return DuckDBError;

// 	if (out) {
// 		memset(out, 0, sizeof(duckdb_result));
// 	}
// 	Connection *conn = (Connection *)connection;
// 	auto result = conn->Query(query);
// 	if (!result->success) {
// 		if (out) {
// 			auto error = result->error;
// 			out->error_message = (char *)malloc(error.size() + 1);
// 			strcpy(out->error_message, error.c_str());
// 		}
// 		result->Print();
// 		return DuckDBError;
// 	}
// 	// construct the C result from the C++ result
// 	if (!out) {
// 		return DuckDBSuccess;
// 	}
// 	out->row_count = result->size();
// 	out->column_count = result->types.size();
// 	out->columns = (duckdb_column *)malloc(out->column_count * sizeof(duckdb_column));
// 	if (!out->columns)
// 		goto mallocfail;
// 	memset(out->columns, 0, out->column_count * sizeof(duckdb_column));

// 	for (size_t i = 0; i < out->column_count; i++) {
// 		auto &types = result->types();

// 		auto type = types[i];
// 		auto type_size = GetTypeIdSize(type);
// 		auto &column = out->columns[i];

// 		column.type = _convert_type_cpp_to_c(type);
// 		column.count = result->size();
// 		column.name = (char *)strdup(result->names[i].c_str());
// 		column.data = (char *)malloc(type_size * result->size());
// 		column.nullmask = (bool *)malloc(sizeof(bool) * result->size());
// 		if (!column.data || !column.nullmask)
// 			goto mallocfail;

// 		// copy the data
// 		if (TypeIsConstantSize(type)) {
// 			char *ptr = column.data;
// 			for (auto &chunk : result->collection.chunks) {
// 				auto &vector = chunk->data[i];
// 				VectorOperations::Copy(vector, ptr);
// 				ptr += type_size * chunk->size();
// 			}
// 		} else {
// 			// NULL initialize: we are going to do mallocs
// 			memset(column.data, 0, type_size * result->size());

// 			if (types[i] == TypeId::VARCHAR) {
// 				char **dataptr = (char **)column.data;
// 				for (auto &chunk : result->collection.chunks) {
// 					auto &vector = chunk->data[i];
// 					const char **str_data = (const char **)vector.data;
// 					for (size_t j = 0; j < chunk->size(); j++) {
// 						const char *strptr = str_data[j];
// 						if (!str_data[j]) {
// 							strptr = "NULL";
// 						}
// 						*dataptr = (char *)malloc(strlen(strptr) + 1);
// 						if (!*dataptr)
// 							goto mallocfail;
// 						strcpy(*dataptr, strptr);
// 						dataptr++;
// 					}
// 				}
// 			} else {
// 				// not supported yet
// 				printf("Copy of non-string varlength values not supported yet!\n");
// 				goto mallocfail;
// 			}
// 		}
// 		size_t index = 0;
// 		// set the nullmask
// 		for (auto &chunk : result->collection.chunks) {
// 			auto &vector = chunk->data[i];
// 			for (size_t i = 0; i < vector.count; i++) {
// 				column.nullmask[index + i] = vector.ValueIsNull(i);
// 			}
// 			index += vector.count;
// 		}
// 	}
// 	return DuckDBSuccess;
// mallocfail:
// 	duckdb_destroy_result(*out);
// 	return DuckDBError;
}

void duckdb_destroy_result(duckdb_result result) {
	if (result.columns) {
		for (size_t i = 0; i < result.column_count; i++) {
			auto &column = result.columns[i];
			if (column.type >= DUCKDB_TYPE_VARCHAR) {
				// variable length size: delete individual elements
				void **dataptr = (void **)column.data;
				for (size_t j = 0; j < result.row_count; j++) {
					if (dataptr[j]) {
						free(dataptr[j]);
					}
				}
			}
			if (column.data) {
				free(column.data);
			}
			if (column.nullmask) {
				free(column.nullmask);
			}
			if (column.name) {
				free(column.name);
			}
		}

		free(result.columns);
	}
	if (result.error_message) {
		free(result.error_message);
	}
}
