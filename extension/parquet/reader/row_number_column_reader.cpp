#include "row_number_column_reader.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Row NumberColumn Reader
//===--------------------------------------------------------------------===//
RowNumberColumnReader::RowNumberColumnReader(ParquetReader &reader, LogicalType type_p, const SchemaElement &schema_p,
											 idx_t schema_idx_p, idx_t max_define_p, idx_t max_repeat_p)
	: ColumnReader(reader, std::move(type_p), schema_p, schema_idx_p, max_define_p, max_repeat_p) {
}

unique_ptr<BaseStatistics> RowNumberColumnReader::Stats(idx_t row_group_idx_p, const vector<ColumnChunk> &columns) {
	auto stats = NumericStats::CreateUnknown(type);
	auto &row_groups = reader.GetFileMetadata()->row_groups;
	D_ASSERT(row_group_idx_p < row_groups.size());
	idx_t row_group_offset_min = 0;
	for (idx_t i = 0; i < row_group_idx_p; i++) {
		row_group_offset_min += row_groups[i].num_rows;
	}

	NumericStats::SetMin(stats, Value::BIGINT(UnsafeNumericCast<int64_t>(row_group_offset_min)));
	NumericStats::SetMax(
		stats, Value::BIGINT(UnsafeNumericCast<int64_t>(row_group_offset_min + row_groups[row_group_idx_p].num_rows)));
	stats.Set(StatsInfo::CANNOT_HAVE_NULL_VALUES);
	return stats.ToUnique();
}

void RowNumberColumnReader::InitializeRead(idx_t row_group_idx_p, const vector<ColumnChunk> &columns,
										   TProtocol &protocol_p) {
	row_group_offset = 0;
	auto &row_groups = reader.GetFileMetadata()->row_groups;
	for (idx_t i = 0; i < row_group_idx_p; i++) {
		row_group_offset += row_groups[i].num_rows;
	}
}

idx_t RowNumberColumnReader::Read(uint64_t num_values, parquet_filter_t &filter, data_ptr_t define_out,
								  data_ptr_t repeat_out, Vector &result) {

	auto data_ptr = FlatVector::GetData<int64_t>(result);
	for (idx_t i = 0; i < num_values; i++) {
		data_ptr[i] = UnsafeNumericCast<int64_t>(row_group_offset++);
	}
	return num_values;
}

}
