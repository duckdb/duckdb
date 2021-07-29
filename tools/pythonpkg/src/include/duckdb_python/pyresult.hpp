//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb_python/pyresult.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb_python/pybind_wrapper.hpp"
#include "duckdb.hpp"

namespace duckdb {

struct DuckDBPyResult {
public:
	idx_t chunk_offset = 0;

	unique_ptr<QueryResult> result;
	unique_ptr<DataChunk> current_chunk;

public:
	static void Initialize(py::handle &m);

	py::object Fetchone();

	py::list Fetchall();

	py::dict FetchNumpy(bool stream = false, idx_t vectors_per_chunk = 1);

	py::object FetchDF();

	py::object FetchDFChunk(idx_t vectors_per_chunk);

	py::object FetchArrowTableChunk(idx_t num_of_vectors = 1);

	py::object FetchArrowTable(bool stream = false, idx_t num_of_vectors = 1);

	py::list Description();

	void Close();
};

} // namespace duckdb
