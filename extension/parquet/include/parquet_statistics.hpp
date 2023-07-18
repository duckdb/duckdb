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

struct ParquetStatisticsUtils {

	static unique_ptr<BaseStatistics> TransformColumnStatistics(const SchemaElement &s_ele, const LogicalType &type,
	                                                            const ColumnChunk &column_chunk);

	static Value ConvertValue(const LogicalType &type, const duckdb_parquet::format::SchemaElement &schema_ele,
	                          const std::string &stats);
};

} // namespace duckdb
