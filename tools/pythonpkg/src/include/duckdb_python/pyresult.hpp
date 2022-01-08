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

public:
	static void Initialize(py::handle &m);

	py::object Fetchone();

	py::list Fetchall();

	py::dict FetchNumpy();

	py::dict FetchNumpyInternal(bool stream = false, idx_t vectors_per_chunk = 1);

	py::object FetchDF();

	py::object FetchDFChunk(idx_t vectors_per_chunk);

	py::object FetchArrowTableChunk(idx_t num_of_vectors = 1, bool return_table = false);

	py::object FetchArrowTable(bool stream = false, idx_t num_of_vectors = 1, bool return_table = true);

	py::object FetchRecordBatchReader(idx_t vectors_per_chunk);

	py::list Description();

	void Close();

	static py::object GetValueToPython(const Value &val, const LogicalType &type);

private:
	void FillNumpy(py::dict &res, idx_t col_idx, NumpyResultConversion &conversion, const char *name);
};

} // namespace duckdb
