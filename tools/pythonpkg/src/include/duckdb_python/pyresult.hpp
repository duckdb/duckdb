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

	explicit DuckDBPyResult() {};

public:
	static void Initialize(py::handle &m);

	py::object Fetchone();

	py::list Fetchall();

	py::dict FetchNumpy();

	py::dict FetchNumpyInternal(bool stream = false, idx_t vectors_per_chunk = 1);

	py::object FetchDF();

	py::object FetchArrowTable(idx_t chunk_size);

	py::object FetchDFChunk(idx_t vectors_per_chunk);

	py::object FetchRecordBatchReader(idx_t chunk_size);

	py::list Description();

	void Close();

	static py::object GetValueToPython(const Value &val, const LogicalType &type);

private:
	void FillNumpy(py::dict &res, idx_t col_idx, NumpyResultConversion &conversion, const char *name);

	py::object FetchAllArrowChunks(idx_t chunk_size);

	bool FetchArrowChunk(QueryResult *result, py::list &batches, idx_t chunk_size);
};

} // namespace duckdb
