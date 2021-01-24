#pragma once

#include "duckdb/storage/statistics/base_statistics.hpp"
#include "parquet_types.h"

namespace duckdb {

using parquet::format::ColumnChunk;
using parquet::format::SchemaElement;

struct LogicalType;

unique_ptr<BaseStatistics> parquet_transform_column_statistics(const SchemaElement &s_ele, const LogicalType &type,
                                                               const ColumnChunk &column_chunk);

} // namespace duckdb
