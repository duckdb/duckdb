#include "parquet_column_schema.hpp"
#include "parquet_reader.hpp"

namespace duckdb {

void ParquetColumnSchema::SetSchemaIndex(idx_t schema_idx) {
	D_ASSERT(!schema_index.IsValid());
	schema_index = schema_idx;
}

//! Writer constructors

ParquetColumnSchema ParquetColumnSchema::FromLogicalType(const string &name, const LogicalType &type, idx_t max_define,
                                                         idx_t max_repeat, idx_t column_index,
                                                         duckdb_parquet::FieldRepetitionType::type repetition_type,
                                                         bool allow_geometry, ParquetColumnSchemaType schema_type) {
	ParquetColumnSchema res;
	res.name = name;
	res.max_define = max_define;
	res.max_repeat = max_repeat;
	res.column_index = column_index;
	res.repetition_type = repetition_type;
	res.schema_type = schema_type;
	res.type = type;
	res.allow_geometry = allow_geometry;
	return res;
}

//! Reader constructors

ParquetColumnSchema ParquetColumnSchema::FromSchemaElement(const duckdb_parquet::SchemaElement &element,
                                                           idx_t max_define, idx_t max_repeat, idx_t schema_index,
                                                           idx_t column_index, ParquetColumnSchemaType schema_type,
                                                           const ParquetOptions &options) {
	ParquetColumnSchema res;
	res.name = element.name;
	res.max_define = max_define;
	res.max_repeat = max_repeat;
	res.schema_index = schema_index;
	res.column_index = column_index;
	res.schema_type = schema_type;
	res.type = ParquetReader::DeriveLogicalType(element, options, res);
	return res;
}

ParquetColumnSchema ParquetColumnSchema::FromParentSchema(ParquetColumnSchema parent, LogicalType result_type,
                                                          ParquetColumnSchemaType schema_type) {
	ParquetColumnSchema res;
	res.name = parent.name;
	res.max_define = parent.max_define;
	res.max_repeat = parent.max_repeat;
	D_ASSERT(parent.schema_index.IsValid());
	res.schema_index = parent.schema_index;
	res.column_index = parent.column_index;
	res.schema_type = schema_type;
	res.type = result_type;
	res.children.push_back(std::move(parent));
	return res;
}

ParquetColumnSchema ParquetColumnSchema::FromChildSchemas(const string &name, const LogicalType &type, idx_t max_define,
                                                          idx_t max_repeat, idx_t schema_index, idx_t column_index,
                                                          vector<ParquetColumnSchema> &&children,
                                                          ParquetColumnSchemaType schema_type) {
	ParquetColumnSchema res;
	res.name = name;
	res.max_define = max_define;
	res.max_repeat = max_repeat;
	res.schema_index = schema_index;
	res.column_index = column_index;
	res.schema_type = schema_type;
	res.type = type;
	res.children = std::move(children);
	return res;
}

ParquetColumnSchema ParquetColumnSchema::FileRowNumber() {
	ParquetColumnSchema res;
	res.name = "file_row_number";
	res.max_define = 0;
	res.max_repeat = 0;
	res.schema_index = 0;
	res.column_index = 0;
	res.schema_type = ParquetColumnSchemaType::FILE_ROW_NUMBER;
	res.type = LogicalType::BIGINT, res.repetition_type = duckdb_parquet::FieldRepetitionType::type::OPTIONAL;
	return res;
}

unique_ptr<BaseStatistics> ParquetColumnSchema::Stats(const FileMetaData &file_meta_data,
                                                      const ParquetOptions &parquet_options, idx_t row_group_idx_p,
                                                      const vector<ColumnChunk> &columns) const {
	if (schema_type == ParquetColumnSchemaType::EXPRESSION) {
		return nullptr;
	}
	if (schema_type == ParquetColumnSchemaType::FILE_ROW_NUMBER) {
		auto stats = NumericStats::CreateUnknown(type);
		auto &row_groups = file_meta_data.row_groups;
		D_ASSERT(row_group_idx_p < row_groups.size());
		idx_t row_group_offset_min = 0;
		for (idx_t i = 0; i < row_group_idx_p; i++) {
			row_group_offset_min += row_groups[i].num_rows;
		}

		NumericStats::SetMin(stats, Value::BIGINT(UnsafeNumericCast<int64_t>(row_group_offset_min)));
		NumericStats::SetMax(stats, Value::BIGINT(UnsafeNumericCast<int64_t>(row_group_offset_min +
		                                                                     row_groups[row_group_idx_p].num_rows)));
		stats.Set(StatsInfo::CANNOT_HAVE_NULL_VALUES);
		return stats.ToUnique();
	}
	return ParquetStatisticsUtils::TransformColumnStatistics(*this, columns, parquet_options.can_have_nan);
}

} // namespace duckdb
