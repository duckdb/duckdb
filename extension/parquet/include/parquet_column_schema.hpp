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

using namespace duckdb_parquet; // NOLINT

using duckdb_parquet::ConvertedType;
using duckdb_parquet::FieldRepetitionType;
using duckdb_parquet::SchemaElement;

using duckdb_parquet::FileMetaData;
struct ParquetOptions;

enum class ParquetColumnSchemaType { COLUMN, FILE_ROW_NUMBER, GEOMETRY, EXPRESSION, VARIANT };

enum class ParquetExtraTypeInfo {
	NONE,
	IMPALA_TIMESTAMP,
	UNIT_NS,
	UNIT_MS,
	UNIT_MICROS,
	DECIMAL_BYTE_ARRAY,
	DECIMAL_INT32,
	DECIMAL_INT64,
	FLOAT16
};

struct ParquetColumnSchema {
public:
	ParquetColumnSchema() = default;
	ParquetColumnSchema(ParquetColumnSchema &&other) = default;
	ParquetColumnSchema(const ParquetColumnSchema &other) = default;
	ParquetColumnSchema &operator=(ParquetColumnSchema &&other) = default;

public:
	//! Writer constructors
	static ParquetColumnSchema FromLogicalType(const string &name, const LogicalType &type, idx_t max_define,
	                                           idx_t max_repeat, idx_t column_index,
	                                           duckdb_parquet::FieldRepetitionType::type repetition_type,
	                                           ParquetColumnSchemaType schema_type = ParquetColumnSchemaType::COLUMN);

public:
	//! Reader constructors
	static ParquetColumnSchema FromSchemaElement(const SchemaElement &element, idx_t max_define, idx_t max_repeat,
	                                             idx_t schema_index, idx_t column_index, ParquetColumnSchemaType type,
	                                             const ParquetOptions &options);
	static ParquetColumnSchema FromParentSchema(ParquetColumnSchema parent, LogicalType result_type,
	                                            ParquetColumnSchemaType schema_type);
	static ParquetColumnSchema FromChildSchemas(const string &name, const LogicalType &type, idx_t max_define,
	                                            idx_t max_repeat, idx_t schema_index, idx_t column_index,
	                                            vector<ParquetColumnSchema> &&children,
	                                            ParquetColumnSchemaType schema_type = ParquetColumnSchemaType::COLUMN);
	static ParquetColumnSchema FileRowNumber();

public:
	unique_ptr<BaseStatistics> Stats(const FileMetaData &file_meta_data, const ParquetOptions &parquet_options,
	                                 idx_t row_group_idx_p, const vector<duckdb_parquet::ColumnChunk> &columns) const;

public:
	void SetSchemaIndex(idx_t schema_idx);

public:
	ParquetColumnSchemaType schema_type;
	string name;
	LogicalType type;
	idx_t max_define;
	idx_t max_repeat;
	//! Populated by FinalizeSchema
	optional_idx schema_index;
	idx_t column_index;
	optional_idx parent_schema_index;
	uint32_t type_length = 0;
	uint32_t type_scale = 0;
	duckdb_parquet::Type::type parquet_type = duckdb_parquet::Type::INT32;
	ParquetExtraTypeInfo type_info = ParquetExtraTypeInfo::NONE;
	vector<ParquetColumnSchema> children;
	optional_idx field_id;
	//! Whether a column is nullable or not
	duckdb_parquet::FieldRepetitionType::type repetition_type;
};

} // namespace duckdb
