#pragma once

#include "duckdb.hpp"
#ifndef DUCKDB_AMALGAMATION
#include "duckdb/storage/statistics/base_statistics.hpp"
#endif
#include "parquet_types.h"

namespace duckdb {

using duckdb_parquet::format::ColumnChunk;
using duckdb_parquet::format::SchemaElement;

struct LogicalType;

unique_ptr<BaseStatistics> ParquetTransformColumnStatistics(const SchemaElement &s_ele, const LogicalType &type,
                                                            const ColumnChunk &column_chunk);

} // namespace duckdb
