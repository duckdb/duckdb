//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/segment/numeric_uncompressed.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/segment/base_segment.hpp"

namespace duckdb {
class DatabaseInstance;

struct NumericUncompressed {
	static CompressionFunction GetFunction(PhysicalType data_type);
};

} // namespace duckdb
