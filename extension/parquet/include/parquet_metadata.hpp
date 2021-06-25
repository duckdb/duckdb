//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parquet_metadata.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parquet_reader.hpp"

namespace duckdb {

class ParquetMetaDataFunction : public TableFunction {
public:
	ParquetMetaDataFunction();
};

class ParquetSchemaFunction : public TableFunction {
public:
	ParquetSchemaFunction();
};

} // namespace duckdb
