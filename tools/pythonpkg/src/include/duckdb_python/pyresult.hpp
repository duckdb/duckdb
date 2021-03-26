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

	py::dict FetchNumpy(bool stream = false);

	py::object FetchDF();

	py::object FetchDFChunk();

	py::object FetchArrowTable();

	py::list Description();

	void Close();
};

}
