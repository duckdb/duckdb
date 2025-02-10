//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parquet_column_schema.hpp
//
//
//===----------------------------------------------------------------------===//
#pragma once

#include "duckdb.hpp"
#include "parquet_types.h"

namespace duckdb {
class ParquetReader;

enum class ParquetColumnSchemaType { COLUMN, CAST, FILE_ROW_NUMBER, GEOMETRY };

enum class ParquetExtraTypeInfo {
	NONE,
	IMPALA_TIMESTAMP,
	UNIT_NS,
	UNIT_MS,
	UNIT_MICROS,
	DECIMAL_BYTE_ARRAY,
	DECIMAL_INT32,
	DECIMAL_INT64
};

struct ParquetColumnSchema {
	ParquetColumnSchema() = default;
	ParquetColumnSchema(idx_t max_define, idx_t max_repeat, idx_t schema_index, idx_t file_index,
	                    ParquetColumnSchemaType schema_type = ParquetColumnSchemaType::COLUMN);
	ParquetColumnSchema(string name, LogicalType type, idx_t max_define, idx_t max_repeat, idx_t schema_index,
	                    idx_t column_index, ParquetColumnSchemaType schema_type = ParquetColumnSchemaType::COLUMN);
	ParquetColumnSchema(ParquetColumnSchema parent, LogicalType cast_type,
	                    ParquetColumnSchemaType schema_type = ParquetColumnSchemaType::CAST);

	ParquetColumnSchemaType schema_type;
	string name;
	LogicalType type;
	idx_t max_define;
	idx_t max_repeat;
	idx_t schema_index;
	idx_t column_index;
	optional_idx parent_schema_index;
	uint32_t type_length = 0;
	uint32_t type_scale = 0;
	duckdb_parquet::Type::type parquet_type = duckdb_parquet::Type::INT32;
	ParquetExtraTypeInfo type_info = ParquetExtraTypeInfo::NONE;
	vector<ParquetColumnSchema> children;

	unique_ptr<BaseStatistics> Stats(ParquetReader &reader, idx_t row_group_idx_p,
	                                 const vector<ColumnChunk> &columns) const;
};

} // namespace duckdb
