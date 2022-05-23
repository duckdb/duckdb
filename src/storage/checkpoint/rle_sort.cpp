#include "duckdb/storage/checkpoint/rle_sort.hpp"

#include "duckdb/storage/table/column_data.hpp"
namespace duckdb {

RowGroupSortBindData::RowGroupSortBindData(const vector<LogicalType> &payload_types,
                                           const vector<LogicalType> &keys_types, vector<column_t> indexes_p,
                                           DatabaseInstance &db_p)
    : payload_types(payload_types), keys_types(keys_types), indexes(std::move(indexes_p)), db(db_p),
      buffer_manager(BufferManager::GetBufferManager(db_p)) {

	// create BoundOrderByNode per column to sort
	for (idx_t i = 0; i < keys_types.size(); ++i) {
		orders.emplace_back(OrderType::ASCENDING, OrderByNullType::NULLS_LAST,
		                    make_unique<BoundReferenceExpression>(keys_types[i], indexes[i]));
	}

	// create payload layout
	payload_layout.Initialize(payload_types);

	// initialize the global sort state
	global_sort_state = make_unique<GlobalSortState>(buffer_manager, orders, payload_layout);
}

unique_ptr<FunctionData> RowGroupSortBindData::Copy() const {
	return make_unique<RowGroupSortBindData>(payload_types, keys_types, indexes, db);
}

bool RowGroupSortBindData::Equals(const FunctionData &other) const {
	return false;
}

RowGroupSortBindData::~RowGroupSortBindData() {
}

RLESort::RLESort(RowGroup &row_group, DataTable &data_table, vector<CompressionType> table_compression)
    : row_group(row_group), data_table(data_table), old_count(row_group.count) {
	// Reorder columns to optimize RLE Compression - We skip if the table has indexes or is empty
	if (row_group.db.config.force_compression_sorting && row_group.count != 0 && row_group.table_info.indexes.Empty()) {
		// collect logical types by iterating the columns
		for (idx_t column_idx = 0; column_idx < row_group.columns.size(); column_idx++) {
			auto &column = row_group.columns[column_idx];
			auto type_id = column->type.id();
			auto column_compression = table_compression[column_idx];
			// We basically only sort columns with RLE compression and that are supported by the RLE algorithm
			if (SupportedSortCompressionType(type_id) && (column_compression == CompressionType::COMPRESSION_RLE)) {
				// Gather types and ids of key columns (i.e., the ones we will sort on)
				key_column_ids.push_back(column_idx);
				key_column_types.push_back(column->type);
			}
			// Gather types and ids of payload columns (i.e., the whole table)
			payload_column_ids.push_back(column_idx);
			payload_column_types.push_back(column->type);
		}
	}
}

bool RLESort::SupportedSortCompressionType(LogicalTypeId type_id) {
	if (type_id == LogicalTypeId::STRUCT || type_id == LogicalTypeId::LIST || type_id == LogicalTypeId::MAP ||
	    type_id == LogicalTypeId::TABLE || type_id == LogicalTypeId::ENUM ||
	    type_id == LogicalTypeId::AGGREGATE_STATE || type_id == LogicalTypeId::VARCHAR ||
	    type_id == LogicalTypeId::BLOB || type_id == LogicalTypeId::INTERVAL || type_id == LogicalTypeId::UUID) {
		return false;
	}
	return true;
}

void RLESort::Initialize() {
	// Initialize the scan states
	scan_state.column_ids = payload_column_ids;
	scan_state.max_row = row_group.count;

	// Initialize the sorting state
	sort_state =
	    make_unique<RowGroupSortBindData>(payload_column_types, key_column_types, key_column_ids, row_group.db);
	local_sort_state.Initialize(*sort_state->global_sort_state, sort_state->global_sort_state->buffer_manager);

	// Scan the RowGroup into the DataChunks
	row_group.InitializeScan(scan_state.row_group_scan_state);
	scan_state.row_group_scan_state.max_row = row_group.count;
}

void RLESort::SinkKeysPayloadSort() {
	while (scan_state.row_group_scan_state.vector_index * STANDARD_VECTOR_SIZE <
	       scan_state.row_group_scan_state.max_row) {
		DataChunk keys_chunk, payload_chunk;
		payload_chunk.Initialize(payload_column_types);
		keys_chunk.Initialize(key_column_types);
		row_group.ScanCommitted(scan_state.row_group_scan_state, payload_chunk,
		                        TableScanType::TABLE_SCAN_COMMITTED_ROWS_OMIT_PERMANENTLY_DELETED_CHECKPOINT);
		payload_chunk.Normalify();
		keys_chunk.ReferencePartial(payload_chunk, key_column_ids);
		local_sort_state.SinkChunk(keys_chunk, payload_chunk);
		new_count += payload_chunk.size();
	}
}

void RLESort::ReplaceRowGroup(RowGroup &sorted_rowgroup) {
	// We have to delete from the data table the difference of chunk counts
	// These refer to deleted tuples
	data_table.total_rows -= row_group.count - new_count;
	row_group.columns = sorted_rowgroup.columns;
	row_group.stats = sorted_rowgroup.stats;
	row_group.version_info = sorted_rowgroup.version_info;
	row_group.count = new_count;
	row_group.start = sorted_rowgroup.start;
	row_group.Verify();
}

unique_ptr<RowGroup> RLESort::CreateSortedRowGroup(GlobalSortState &global_sort_state) {
	// Initialize sorted rowgroup
	auto sorted_rowgroup = make_unique<RowGroup>(row_group.db, row_group.table_info, data_table.prev_end, new_count);
	sorted_rowgroup->InitializeEmpty(payload_column_types);
	TableAppendState append_state;
	sorted_rowgroup->InitializeAppendInternal(append_state.row_group_append_state, new_count);
	// Create the scanner for the sorting
	PayloadScanner scanner(*global_sort_state.sorted_blocks[0]->payload_data, global_sort_state);

	for (;;) {
		// Scan all chunks resulting from the sorting
		DataChunk result_chunk;
		result_chunk.Initialize(payload_column_types);
		result_chunk.SetCardinality(0);
		scanner.Scan(result_chunk);
		if (result_chunk.size() == 0) {
			break;
		}
		result_chunk.SetCardinality(result_chunk.size());
		// Append each chunk in our sorted rowgroup
		sorted_rowgroup->Append(append_state.row_group_append_state, result_chunk, result_chunk.size());
	}
	return sorted_rowgroup;
}

void RLESort::Sort() {
	if (key_column_ids.empty()) {
		// Nothing to sort on
		return;
	}
	Initialize();
	SinkKeysPayloadSort();
	if (new_count == 0) {
		// No changes
		return;
	}

	// add local state to global state, which sorts the data
	auto &global_sort_state = *sort_state->global_sort_state;
	global_sort_state.AddLocalState(local_sort_state);
	global_sort_state.PrepareMergePhase();

	for (idx_t column_idx = 0; column_idx < row_group.columns.size(); column_idx++) {
		row_group.columns[column_idx]->CleanPersistentSegments();
	}

	// scan the sorted row data and add to the sorted row group
	int64_t count_change = new_count - old_count;
	data_table.rows_changed += count_change;

	// Initialize Sorted Row Group
	auto sorted_rowgroup = CreateSortedRowGroup(global_sort_state);

	data_table.prev_end += new_count;
	ReplaceRowGroup(*sorted_rowgroup);
}
} // namespace duckdb
