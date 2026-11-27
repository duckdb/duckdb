//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/table/triangle_count.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/built_in_functions.hpp"
#include "duckdb/function/table_function.hpp"

namespace duckdb {

//! triangle_count(table_name, src_column, dst_column)
//! Counts the triangles of the undirected graph described by an edge table.
struct TriangleCountTableFunction {
	static void RegisterFunction(BuiltinFunctions &set);
};

} // namespace duckdb
