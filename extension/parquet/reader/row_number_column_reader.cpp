#include "reader/row_number_column_reader.hpp"

#include "parquet_reader.hpp"
#include "duckdb/storage/table/row_group.hpp"
#include "duckdb/common/enums/filter_propagate_result.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "parquet_types.h"

namespace duckdb_apache {
namespace thrift {
namespace protocol {
class TProtocol;
} // namespace protocol
} // namespace thrift
} // namespace duckdb_apache

namespace duckdb {
class TableFilter;
class Vector;
struct ParquetColumnSchema;
struct SelectionVector;
struct TableFilterState;

//===--------------------------------------------------------------------===//
// Row NumberColumn Reader
//===--------------------------------------------------------------------===//
RowNumberColumnReader::RowNumberColumnReader(const ParquetReader &reader, const ParquetColumnSchema &schema,
                                             const ColumnIndex &column_id)
    : ColumnReader(reader, schema, column_id) {
}

void RowNumberColumnReader::InitializeRead(idx_t row_group_idx_p, const vector<ColumnChunk> &columns,
                                           TProtocol &protocol_p) {
	row_group_offset = 0;
	auto &row_groups = reader.GetFileMetadata()->row_groups;
	for (idx_t i = 0; i < row_group_idx_p; i++) {
		row_group_offset += row_groups[i].num_rows;
	}
}

void RowNumberColumnReader::Filter(ColumnReaderInput &input, Vector &result, const TableFilter &filter,
                                   TableFilterState &filter_state, SelectionVector &sel, idx_t &approved_tuple_count,
                                   bool is_first_filter) {
	// check the row id stats if this filter has any chance of passing

	auto &num_values = input.num_values;
	auto prune_result = RowGroup::CheckRowIdFilter(filter, row_group_offset, row_group_offset + num_values);
	if (prune_result == FilterPropagateResult::FILTER_ALWAYS_FALSE) {
		// filter is always false - don't read anything
		approved_tuple_count = 0;
		Skip(num_values);
		return;
	}
	ColumnReader::Filter(input, result, filter, filter_state, sel, approved_tuple_count, is_first_filter);
}

idx_t RowNumberColumnReader::Read(ColumnReaderInput &input, Vector &result) {
	auto &num_values = input.num_values;

	auto data_ptr = FlatVector::Writer<int64_t>(result, num_values);
	for (idx_t i = 0; i < num_values; i++) {
		data_ptr.WriteValue(UnsafeNumericCast<int64_t>(row_group_offset++));
	}
	return num_values;
}

} // namespace duckdb
