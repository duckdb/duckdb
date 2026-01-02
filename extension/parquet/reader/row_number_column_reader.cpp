#include "reader/row_number_column_reader.hpp"
#include "parquet_reader.hpp"
#include "duckdb/storage/table/row_group.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Row NumberColumn Reader
//===--------------------------------------------------------------------===//
RowNumberColumnReader::RowNumberColumnReader(ParquetReader &reader, const ParquetColumnSchema &schema)
    : ColumnReader(reader, schema) {
}

void RowNumberColumnReader::InitializeRead(idx_t row_group_idx_p, const vector<ColumnChunk> &columns,
                                           TProtocol &protocol_p) {
	row_group_offset = 0;
	auto &row_groups = reader.GetFileMetadata()->row_groups;
	for (idx_t i = 0; i < row_group_idx_p; i++) {
		row_group_offset += row_groups[i].num_rows;
	}
}

void RowNumberColumnReader::Filter(uint64_t num_values, data_ptr_t define_out, data_ptr_t repeat_out,
                                   Vector &result_out, const TableFilter &filter, TableFilterState &filter_state,
                                   SelectionVector &sel, idx_t &approved_tuple_count, bool is_first_filter) {
	// check the row id stats if this filter has any chance of passing
	auto prune_result = RowGroup::CheckRowIdFilter(filter, row_group_offset, row_group_offset + num_values);
	if (prune_result == FilterPropagateResult::FILTER_ALWAYS_FALSE) {
		// filter is always false - don't read anything
		approved_tuple_count = 0;
		Skip(num_values);
		return;
	}
	ColumnReader::Filter(num_values, define_out, repeat_out, result_out, filter, filter_state, sel,
	                     approved_tuple_count, is_first_filter);
}

idx_t RowNumberColumnReader::Read(uint64_t num_values, data_ptr_t define_out, data_ptr_t repeat_out, Vector &result) {
	auto data_ptr = FlatVector::GetData<int64_t>(result);
	for (idx_t i = 0; i < num_values; i++) {
		data_ptr[i] = UnsafeNumericCast<int64_t>(row_group_offset++);
	}
	return num_values;
}

} // namespace duckdb
