#pragma once

#include "duckdb.hpp"
#ifndef DUCKDB_AMALGAMATION
#include "duckdb/storage/statistics/base_statistics.hpp"
#endif
#include "parquet_types.h"

namespace duckdb {

using duckdb_parquet::ColumnChunk;
using duckdb_parquet::SchemaElement;

struct LogicalType;
class ColumnReader;

struct ParquetStatisticsUtils {

	static unique_ptr<BaseStatistics> TransformColumnStatistics(const ColumnReader &reader,
	                                                            const vector<ColumnChunk> &columns);

	static Value ConvertValue(const LogicalType &type, const duckdb_parquet::SchemaElement &schema_ele,
	                          const std::string &stats);

	static bool BloomFilterExcludes(const TableFilter &filter, const duckdb_parquet::ColumnMetaData &column_meta_data,
	                                duckdb_apache::thrift::protocol::TProtocol &file_proto, Allocator &allocator);
};

} // namespace duckdb
