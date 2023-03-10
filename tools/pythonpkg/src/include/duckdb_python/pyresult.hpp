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
#include "duckdb_python/python_objects.hpp"

namespace duckdb {

struct DuckDBPyResult {
public:
	explicit DuckDBPyResult(unique_ptr<QueryResult> result);

public:
	Optional<py::tuple> Fetchone();

	py::list Fetchmany(idx_t size);

	py::list Fetchall();

	py::dict FetchNumpy();

	py::dict FetchNumpyInternal(bool stream = false, idx_t vectors_per_chunk = 1);

	DataFrame FetchDF(bool date_as_object);

	duckdb::pyarrow::Table FetchArrowTable(idx_t chunk_size);

	DataFrame FetchDFChunk(idx_t vectors_per_chunk, bool date_as_object);

	py::dict FetchPyTorch();

	py::dict FetchTF();

	duckdb::pyarrow::RecordBatchReader FetchRecordBatchReader(idx_t chunk_size);

	static py::list GetDescription(const vector<string> &names, const vector<LogicalType> &types);

	void Close();

	bool IsClosed() const;

	unique_ptr<DataChunk> FetchChunk();

	const vector<string> &GetNames();
	const vector<LogicalType> &GetTypes();

private:
	void FillNumpy(py::dict &res, idx_t col_idx, NumpyResultConversion &conversion, const char *name);

	py::list FetchAllArrowChunks(idx_t chunk_size);

	bool FetchArrowChunk(QueryResult *result, py::list &batches, idx_t chunk_size);

	DataFrame FrameFromNumpy(bool date_as_object, const py::handle &o);

	void ChangeToTZType(DataFrame &df);
	void ChangeDateToDatetime(DataFrame &df);
	unique_ptr<DataChunk> FetchNext(QueryResult &result);
	unique_ptr<DataChunk> FetchNextRaw(QueryResult &result);

private:
	idx_t chunk_offset = 0;

	unique_ptr<QueryResult> result;
	unique_ptr<DataChunk> current_chunk;
	// Holds the categories of Categorical/ENUM types
	unordered_map<idx_t, py::list> categories;
	// Holds the categorical type of Categorical/ENUM types
	unordered_map<idx_t, py::object> categories_type;

	string timezone_config;

	bool result_closed = false;
};

} // namespace duckdb
