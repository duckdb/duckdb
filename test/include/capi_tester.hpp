#pragma once

#include "catch.hpp"
#include "duckdb.h"
#include "test_helpers.hpp"
#include "duckdb/common/arrow/arrow.hpp"
#include "duckdb/common/exception.hpp"

namespace duckdb {

class CAPIDataChunk {
public:
	CAPIDataChunk(duckdb_data_chunk chunk_p) : chunk(chunk_p) {
	}
	~CAPIDataChunk() {
		duckdb_destroy_data_chunk(&chunk);
	}

	idx_t ColumnCount() {
		return duckdb_data_chunk_get_column_count(chunk);
	}
	idx_t size() {
		return duckdb_data_chunk_get_size(chunk);
	}
	duckdb_vector GetVector(idx_t col) {
		return duckdb_data_chunk_get_vector(chunk, col);
	}
	void *GetData(idx_t col) {
		return duckdb_vector_get_data(GetVector(col));
	}
	uint64_t *GetValidity(idx_t col) {
		return duckdb_vector_get_validity(GetVector(col));
	}
	duckdb_data_chunk GetChunk() {
		return chunk;
	}

private:
	duckdb_data_chunk chunk;
};

class CAPIResult {
public:
	CAPIResult() {
	}
	CAPIResult(duckdb_result result, bool success) : success(success), result(result) {
	}
	~CAPIResult() {
		duckdb_destroy_result(&result);
	}

public:
	bool HasError() const {
		return !success;
	}
	void Query(duckdb_connection connection, string query) {
		success = (duckdb_query(connection, query.c_str(), &result) == DuckDBSuccess);
		if (!success) {
			REQUIRE(ErrorMessage() != nullptr);
		}
	}

	duckdb_type ColumnType(idx_t col) {
		return duckdb_column_type(&result, col);
	}

	idx_t ChunkCount() {
		return duckdb_result_chunk_count(result);
	}

	unique_ptr<CAPIDataChunk> FetchChunk(idx_t chunk_idx) {
		auto chunk = duckdb_result_get_chunk(result, chunk_idx);
		if (!chunk) {
			return nullptr;
		}
		return make_unique<CAPIDataChunk>(chunk);
	}

	template <class T>
	T *ColumnData(idx_t col) {
		return (T *)duckdb_column_data(&result, col);
	}

	idx_t ColumnCount() {
		return duckdb_column_count(&result);
	}

	idx_t row_count() {
		return duckdb_row_count(&result);
	}

	idx_t rows_changed() {
		return duckdb_rows_changed(&result);
	}

	template <class T>
	T Fetch(idx_t col, idx_t row) {
		throw NotImplementedException("Unimplemented type for fetch");
	}

	bool IsNull(idx_t col, idx_t row) {
		auto nullmask_ptr = duckdb_nullmask_data(&result, col);
		REQUIRE(duckdb_value_is_null(&result, col, row) == nullmask_ptr[row]);
		return nullmask_ptr[row];
	}

	const char *ErrorMessage() {
		return duckdb_result_error(&result);
	}

	string ColumnName(idx_t col) {
		auto colname = duckdb_column_name(&result, col);
		return colname ? string(colname) : string();
	}

	duckdb_result &InternalResult() {
		return result;
	}

public:
	bool success = false;

private:
	duckdb_result result;
};

template <>
bool CAPIResult::Fetch(idx_t col, idx_t row);
template <>
int8_t CAPIResult::Fetch(idx_t col, idx_t row);
template <>
int16_t CAPIResult::Fetch(idx_t col, idx_t row);
template <>
int32_t CAPIResult::Fetch(idx_t col, idx_t row);
template <>
int64_t CAPIResult::Fetch(idx_t col, idx_t row);
template <>
uint8_t CAPIResult::Fetch(idx_t col, idx_t row);
template <>
uint16_t CAPIResult::Fetch(idx_t col, idx_t row);
template <>
uint32_t CAPIResult::Fetch(idx_t col, idx_t row);
template <>
uint64_t CAPIResult::Fetch(idx_t col, idx_t row);
template <>
float CAPIResult::Fetch(idx_t col, idx_t row);
template <>
double CAPIResult::Fetch(idx_t col, idx_t row);
template <>
duckdb_decimal CAPIResult::Fetch(idx_t col, idx_t row);
template <>
duckdb_date CAPIResult::Fetch(idx_t col, idx_t row);
template <>
duckdb_time CAPIResult::Fetch(idx_t col, idx_t row);
template <>
duckdb_timestamp CAPIResult::Fetch(idx_t col, idx_t row);
template <>
duckdb_interval CAPIResult::Fetch(idx_t col, idx_t row);
template <>
duckdb_blob CAPIResult::Fetch(idx_t col, idx_t row);
template <>
string CAPIResult::Fetch(idx_t col, idx_t row);
template <>
duckdb_date_struct CAPIResult::Fetch(idx_t col, idx_t row);
template <>
duckdb_time_struct CAPIResult::Fetch(idx_t col, idx_t row);
template <>
duckdb_timestamp_struct CAPIResult::Fetch(idx_t col, idx_t row);
template <>
duckdb_hugeint CAPIResult::Fetch(idx_t col, idx_t row);

class CAPITester {
public:
	CAPITester() : database(nullptr), connection(nullptr) {
	}
	~CAPITester() {
		Cleanup();
	}

	void Cleanup() {
		if (connection) {
			duckdb_disconnect(&connection);
			connection = nullptr;
		}
		if (database) {
			duckdb_close(&database);
			database = nullptr;
		}
	}

	bool OpenDatabase(const char *path) {
		Cleanup();
		if (duckdb_open(path, &database) != DuckDBSuccess) {
			return false;
		}
		if (duckdb_connect(database, &connection) != DuckDBSuccess) {
			return false;
		}
		return true;
	}

	unique_ptr<CAPIResult> Query(string query) {
		D_ASSERT(connection);
		auto result = make_unique<CAPIResult>();
		result->Query(connection, query);
		return result;
	}

	duckdb_database database = nullptr;
	duckdb_connection connection = nullptr;
};

} // namespace duckdb

bool NO_FAIL(duckdb::CAPIResult &result);
bool NO_FAIL(duckdb::unique_ptr<duckdb::CAPIResult> result);
