//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb_python/pyresult.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "array_wrapper.hpp"
#include "duckdb.hpp"
#include "duckdb_python/pybind_wrapper.hpp"

namespace duckdb {

struct DuckDBPyResult {
public:
	idx_t chunk_offset = 0;

	unique_ptr<QueryResult> result;
	unique_ptr<DataChunk> current_chunk;
	// Holds the categories of Categorical/ENUM types
	unordered_map<idx_t, py::list> categories;
	// Holds the categorical type of Categorical/ENUM types
	unordered_map<idx_t, py::object> categories_type;

	string timezone_config;

	explicit DuckDBPyResult() {};

public:
	static void Initialize(py::handle &m);

	py::object Fetchone();

	py::list Fetchmany(idx_t size);

	py::list Fetchall();

	py::dict FetchNumpy();

	py::dict FetchNumpyInternal(bool stream = false, idx_t vectors_per_chunk = 1);

	DataFrame FetchDF(bool date_as_object);

	duckdb::pyarrow::Table FetchArrowTable(idx_t chunk_size);

	DataFrame FetchDFChunk(idx_t vectors_per_chunk, bool date_as_object);

	duckdb::pyarrow::RecordBatchReader FetchRecordBatchReader(idx_t chunk_size);

	py::list Description();

	void Close();

	static py::object FromValue(const Value &val, const LogicalType &type);

private:
	void FillNumpy(py::dict &res, idx_t col_idx, NumpyResultConversion &conversion, const char *name);

	py::object FetchAllArrowChunks(idx_t chunk_size);

	bool FetchArrowChunk(QueryResult *result, py::list &batches, idx_t chunk_size);

	DataFrame FrameFromNumpy(bool date_as_object, const py::handle &o);

	void ChangeToTZType(DataFrame &df);
	void ChangeDateToDatetime(DataFrame &df);
	unique_ptr<DataChunk> FetchNext(QueryResult &result);
	unique_ptr<DataChunk> FetchNextRaw(QueryResult &result);

private:
	bool result_closed = false;
};

} // namespace duckdb
