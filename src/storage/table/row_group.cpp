#include "duckdb/storage/table/row_group.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/serializer/binary_serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/execution/adaptive_filter.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/storage/checkpoint/table_data_writer.hpp"
#include "duckdb/storage/metadata/metadata_reader.hpp"
#include "duckdb/storage/table/append_state.hpp"
#include "duckdb/storage/table/column_checkpoint_state.hpp"
#include "duckdb/storage/table/column_data.hpp"
#include "duckdb/storage/table/row_version_manager.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/storage/table/update_segment.hpp"
#include "duckdb/storage/table_storage_info.hpp"
#include "duckdb/transaction/duck_transaction.hpp"
#include "duckdb/transaction/duck_transaction_manager.hpp"
#include "duckdb/storage/table/row_id_column_data.hpp"
#include "duckdb/main/settings.hpp"

namespace duckdb {

RowGroup::RowGroup(RowGroupCollection &collection_p, idx_t count)
    : SegmentBase<RowGroup>(count), collection(collection_p), version_info(nullptr), deletes_is_loaded(false),
      allocation_size(0), row_id_is_loaded(false), has_changes(false) {
	Verify();
}

RowGroup::RowGroup(RowGroupCollection &collection_p, RowGroupPointer pointer)
    : SegmentBase<RowGroup>(pointer.tuple_count), collection(collection_p), version_info(nullptr),
      deletes_is_loaded(false), allocation_size(0), row_id_is_loaded(false), has_changes(false) {
	// deserialize the columns
	if (pointer.data_pointers.size() != collection_p.GetTypes().size()) {
		throw IOException("Row group column count is unaligned with table column count. Corrupt file?");
	}
	this->column_pointers = std::move(pointer.data_pointers);
	this->columns.resize(column_pointers.size());
	this->is_loaded = unique_ptr<atomic<bool>[]>(new atomic<bool>[columns.size()]);
	for (idx_t c = 0; c < columns.size(); c++) {
		this->is_loaded[c] = false;
	}
	this->deletes_pointers = std::move(pointer.deletes_pointers);
	this->has_metadata_blocks = pointer.has_metadata_blocks;
	this->extra_metadata_blocks = std::move(pointer.extra_metadata_blocks);

	Verify();
}

RowGroup::RowGroup(RowGroupCollection &collection_p, PersistentRowGroupData &data)
    : SegmentBase<RowGroup>(data.count), collection(collection_p), version_info(nullptr), deletes_is_loaded(false),
      allocation_size(0), row_id_is_loaded(false), has_changes(false) {
	auto &block_manager = GetBlockManager();
	auto &info = GetTableInfo();
	auto &types = collection.get().GetTypes();
	columns.reserve(types.size());
	for (idx_t c = 0; c < types.size(); c++) {
		auto entry = ColumnData::CreateColumn(block_manager, info, c, types[c]);
		entry->InitializeColumn(data.column_data[c]);
		columns.push_back(std::move(entry));
	}

	Verify();
}

void RowGroup::MoveToCollection(RowGroupCollection &collection_p) {
	lock_guard<mutex> l(row_group_lock);
	// FIXME
	// MoveToCollection causes any_changes to be set to true because we are changing the start position of the row group
	// the start position is ONLY written when targeting old serialization versions - as such, we don't actually
	// need to do this when targeting newer serialization versions
	// not doing this could allow metadata reuse in these situations, which would improve vacuuming performance
	// especially when vacuuming from the beginning of large tables
	has_changes = true;
	this->collection = collection_p;
	for (idx_t c = 0; c < columns.size(); c++) {
		if (is_loaded && !is_loaded[c]) {
			// we only need to set the column start position if it is already loaded
			// if it is not loaded - we will set the correct start position upon loading
			continue;
		}
		columns[c]->SetDataType(ColumnDataType::MAIN_TABLE);
	}
}

RowGroup::~RowGroup() {
}

vector<shared_ptr<ColumnData>> &RowGroup::GetColumns() {
	// ensure all columns are loaded
	for (idx_t c = 0; c < GetColumnCount(); c++) {
		GetColumn(c);
	}
	return columns;
}

idx_t RowGroup::GetColumnCount() const {
	return columns.size();
}

idx_t RowGroup::GetRowGroupSize() const {
	return collection.get().GetRowGroupSize();
}

void RowGroup::LoadRowIdColumnData() const {
	if (row_id_is_loaded) {
		return;
	}
	lock_guard<mutex> l(row_group_lock);
	if (row_id_column_data) {
		return;
	}
	row_id_column_data = make_uniq<RowIdColumnData>(GetBlockManager(), GetTableInfo());
	row_id_column_data->count = count.load();
	row_id_is_loaded = true;
}

ColumnData &RowGroup::GetColumn(const StorageIndex &c) const {
	return GetColumn(c.GetPrimaryIndex());
}

ColumnData &RowGroup::GetColumn(storage_t c) const {
	LoadColumn(c);
	return c == COLUMN_IDENTIFIER_ROW_ID ? *row_id_column_data : *columns[c];
}

void RowGroup::LoadColumn(storage_t c) const {
	if (c == COLUMN_IDENTIFIER_ROW_ID) {
		LoadRowIdColumnData();
		return;
	}
	D_ASSERT(c < columns.size());
	if (!is_loaded) {
		// not being lazy loaded
		D_ASSERT(columns[c]);
		return;
	}
	if (is_loaded[c]) {
		D_ASSERT(columns[c]);
		return;
	}
	lock_guard<mutex> l(row_group_lock);
	if (columns[c]) {
		// another thread loaded the column while we were waiting for the lock
		D_ASSERT(is_loaded[c]);
		return;
	}
	// load the column
	if (column_pointers.size() != columns.size()) {
		throw InternalException("Lazy loading a column but the pointer was not set");
	}
	auto &metadata_manager = GetCollection().GetMetadataManager();
	auto &types = GetCollection().GetTypes();
	auto &block_pointer = column_pointers[c];
	MetadataReader column_data_reader(metadata_manager, block_pointer);
	this->columns[c] = ColumnData::Deserialize(GetBlockManager(), GetTableInfo(), c, column_data_reader, types[c]);
	is_loaded[c] = true;
	if (this->columns[c]->count != this->count) {
		throw InternalException("Corrupted database - loaded column with index %llu, count %llu did "
		                        "not match count of row group %llu",
		                        c, this->columns[c]->count.load(), this->count.load());
	}
}

BlockManager &RowGroup::GetBlockManager() const {
	return GetCollection().GetBlockManager();
}
DataTableInfo &RowGroup::GetTableInfo() const {
	return GetCollection().GetTableInfo();
}

void RowGroup::InitializeEmpty(const vector<LogicalType> &types, ColumnDataType data_type) {
	// set up the segment trees for the column segments
	D_ASSERT(columns.empty());
	for (idx_t i = 0; i < types.size(); i++) {
		auto column_data = ColumnData::CreateColumn(GetBlockManager(), GetTableInfo(), i, types[i], data_type);
		columns.push_back(std::move(column_data));
	}
}

void ColumnScanState::Initialize(const QueryContext &context_p, const LogicalType &type,
                                 const vector<StorageIndex> &children, optional_ptr<TableScanOptions> options) {
	// Register the options in the state
	scan_options = options;
	context = context_p;

	if (type.id() == LogicalTypeId::VALIDITY) {
		// validity - nothing to initialize
		return;
	}
	D_ASSERT(child_states.empty());
	if (type.InternalType() == PhysicalType::STRUCT) {
		// validity + struct children
		auto &struct_children = StructType::GetChildTypes(type);
		child_states.reserve(struct_children.size() + 1);
		for (idx_t i = 0; i <= struct_children.size(); i++) {
			child_states.emplace_back(parent);
		}

		if (children.empty()) {
			// scan all struct children
			scan_child_column.resize(struct_children.size(), true);
			for (idx_t i = 0; i < struct_children.size(); i++) {
				child_states[i + 1].Initialize(context, struct_children[i].second, options);
			}
		} else {
			// only scan the specified subset of columns
			scan_child_column.resize(struct_children.size(), false);
			for (idx_t i = 0; i < children.size(); i++) {
				auto &child = children[i];
				auto index = child.GetPrimaryIndex();
				auto &child_indexes = child.GetChildIndexes();
				scan_child_column[index] = true;
				child_states[index + 1].Initialize(context, struct_children[index].second, child_indexes, options);
			}
		}
		child_states[0].scan_options = options;
	} else if (type.InternalType() == PhysicalType::LIST) {
		// validity + list child
		for (idx_t i = 0; i < 2; i++) {
			child_states.emplace_back(parent);
		}
		child_states[1].Initialize(context, ListType::GetChildType(type), options);
		child_states[0].scan_options = options;
	} else if (type.InternalType() == PhysicalType::ARRAY) {
		// validity + array child
		for (idx_t i = 0; i < 2; i++) {
			child_states.emplace_back(parent);
		}
		child_states[0].scan_options = options;
		child_states[1].Initialize(context, ArrayType::GetChildType(type), options);
	} else {
		// validity
		child_states.emplace_back(parent);
		child_states[0].scan_options = options;
	}
}

void ColumnScanState::Initialize(const QueryContext &context_p, const LogicalType &type,
                                 optional_ptr<TableScanOptions> options) {
	vector<StorageIndex> children;
	Initialize(context_p, type, children, options);
}

void CollectionScanState::Initialize(const QueryContext &context, const vector<LogicalType> &types) {
	auto &column_ids = GetColumnIds();
	column_scans.reserve(column_scans.size());
	for (idx_t i = 0; i < column_ids.size(); i++) {
		column_scans.emplace_back(*this);
	}
	for (idx_t i = 0; i < column_ids.size(); i++) {
		if (column_ids[i].IsRowIdColumn()) {
			continue;
		}
		auto col_id = column_ids[i].GetPrimaryIndex();
		column_scans[i].Initialize(context, types[col_id], column_ids[i].GetChildIndexes(), &GetOptions());
	}
}

bool RowGroup::InitializeScanWithOffset(CollectionScanState &state, SegmentNode<RowGroup> &node, idx_t vector_offset) {
	auto &column_ids = state.GetColumnIds();
	auto &filters = state.GetFilterInfo();
	if (!CheckZonemap(filters)) {
		return false;
	}
	if (!RefersToSameObject(*node.node, *this)) {
		throw InternalException("RowGroup::InitializeScanWithOffset segment node mismatch");
	}

	state.row_group = node;
	state.vector_index = vector_offset;
	auto row_start = node.row_start;
	state.max_row_group_row = row_start > state.max_row ? 0 : MinValue<idx_t>(this->count, state.max_row - row_start);
	auto row_number = vector_offset * STANDARD_VECTOR_SIZE;
	if (state.max_row_group_row == 0) {
		// exceeded row groups to scan
		return false;
	}
	D_ASSERT(!state.column_scans.empty());
	for (idx_t i = 0; i < column_ids.size(); i++) {
		const auto &column = column_ids[i];
		auto &column_data = GetColumn(column);
		column_data.InitializeScanWithOffset(state.column_scans[i], row_number);
		state.column_scans[i].scan_options = &state.GetOptions();
	}
	return true;
}

bool RowGroup::InitializeScan(CollectionScanState &state, SegmentNode<RowGroup> &node) {
	auto &column_ids = state.GetColumnIds();
	auto &filters = state.GetFilterInfo();
	if (!CheckZonemap(filters)) {
		return false;
	}
	if (!RefersToSameObject(*node.node, *this)) {
		throw InternalException("RowGroup::InitializeScan segment node mismatch");
	}
	auto row_start = node.row_start;
	state.row_group = node;
	state.vector_index = 0;
	state.max_row_group_row = row_start > state.max_row ? 0 : MinValue<idx_t>(this->count, state.max_row - row_start);
	if (state.max_row_group_row == 0) {
		return false;
	}
	D_ASSERT(!state.column_scans.empty());
	for (idx_t i = 0; i < column_ids.size(); i++) {
		auto column = column_ids[i];
		auto &column_data = GetColumn(column);
		column_data.InitializeScan(state.column_scans[i]);
		state.column_scans[i].scan_options = &state.GetOptions();
	}
	return true;
}

unique_ptr<RowGroup> RowGroup::AlterType(RowGroupCollection &new_collection, const LogicalType &target_type,
                                         idx_t changed_idx, ExpressionExecutor &executor,
                                         CollectionScanState &scan_state, SegmentNode<RowGroup> &node,
                                         DataChunk &scan_chunk) {
	Verify();

	// construct a new column data for this type
	auto column_data = ColumnData::CreateColumn(GetBlockManager(), GetTableInfo(), changed_idx, target_type);

	ColumnAppendState append_state;
	column_data->InitializeAppend(append_state);

	// scan the original table, and fill the new column with the transformed value
	scan_state.Initialize(executor.GetContext(), GetCollection().GetTypes());
	InitializeScan(scan_state, node);

	DataChunk append_chunk;
	vector<LogicalType> append_types;
	append_types.push_back(target_type);
	append_chunk.Initialize(Allocator::DefaultAllocator(), append_types);
	auto &append_vector = append_chunk.data[0];
	while (true) {
		// scan the table
		scan_chunk.Reset();
		ScanCommitted(scan_state, scan_chunk, TableScanType::TABLE_SCAN_COMMITTED_ROWS);
		if (scan_chunk.size() == 0) {
			break;
		}
		// execute the expression
		append_chunk.Reset();
		executor.ExecuteExpression(scan_chunk, append_vector);
		column_data->Append(append_state, append_vector, scan_chunk.size());
	}

	// set up the row_group based on this row_group
	auto row_group = make_uniq<RowGroup>(new_collection, this->count);
	row_group->SetVersionInfo(GetOrCreateVersionInfoPtr());
	auto &cols = GetColumns();
	for (idx_t i = 0; i < cols.size(); i++) {
		if (i == changed_idx) {
			// this is the altered column: use the new column
			row_group->columns.push_back(std::move(column_data));
			column_data.reset();
		} else {
			// this column was not altered: use the data directly
			row_group->columns.push_back(cols[i]);
		}
	}
	row_group->Verify();
	return row_group;
}

unique_ptr<RowGroup> RowGroup::AddColumn(RowGroupCollection &new_collection, ColumnDefinition &new_column,
                                         ExpressionExecutor &executor, Vector &result) {
	Verify();

	// construct a new column data for the new column
	auto added_column =
	    ColumnData::CreateColumn(GetBlockManager(), GetTableInfo(), GetColumnCount(), new_column.Type());

	idx_t rows_to_write = this->count;
	if (rows_to_write > 0) {
		DataChunk dummy_chunk;

		ColumnAppendState state;
		added_column->InitializeAppend(state);
		for (idx_t i = 0; i < rows_to_write; i += STANDARD_VECTOR_SIZE) {
			idx_t rows_in_this_vector = MinValue<idx_t>(rows_to_write - i, STANDARD_VECTOR_SIZE);
			dummy_chunk.SetCardinality(rows_in_this_vector);
			executor.ExecuteExpression(dummy_chunk, result);
			added_column->Append(state, result, rows_in_this_vector);
		}
	}

	// set up the row_group based on this row_group
	auto row_group = make_uniq<RowGroup>(new_collection, this->count);
	row_group->SetVersionInfo(GetOrCreateVersionInfoPtr());
	row_group->columns = GetColumns();
	// now add the new column
	row_group->columns.push_back(std::move(added_column));

	row_group->Verify();
	return row_group;
}

unique_ptr<RowGroup> RowGroup::RemoveColumn(RowGroupCollection &new_collection, idx_t removed_column) {
	Verify();

	D_ASSERT(removed_column < columns.size());

	auto row_group = make_uniq<RowGroup>(new_collection, this->count);
	row_group->SetVersionInfo(GetOrCreateVersionInfoPtr());
	// copy over all columns except for the removed one
	auto &cols = GetColumns();
	for (idx_t i = 0; i < cols.size(); i++) {
		if (i != removed_column) {
			row_group->columns.push_back(cols[i]);
		}
	}

	row_group->Verify();
	return row_group;
}

void RowGroup::CommitDrop() {
	for (idx_t column_idx = 0; column_idx < GetColumnCount(); column_idx++) {
		CommitDropColumn(column_idx);
	}
}

void RowGroup::CommitDropColumn(const idx_t column_index) {
	auto &column = GetColumn(column_index);
	column.CommitDropColumn();
}

void RowGroup::NextVector(CollectionScanState &state) {
	state.vector_index++;
	const auto &column_ids = state.GetColumnIds();
	for (idx_t i = 0; i < column_ids.size(); i++) {
		const auto &column = column_ids[i];
		GetColumn(column).Skip(state.column_scans[i]);
	}
}

FilterPropagateResult RowGroup::CheckRowIdFilter(const TableFilter &filter, idx_t beg_row, idx_t end_row) {
	// RowId columns dont have a zonemap, but we can trivially create stats to check the filter against.
	BaseStatistics dummy_stats = NumericStats::CreateEmpty(LogicalType::ROW_TYPE);
	NumericStats::SetMin(dummy_stats, UnsafeNumericCast<row_t>(beg_row));
	NumericStats::SetMax(dummy_stats, UnsafeNumericCast<row_t>(end_row));

	return filter.CheckStatistics(dummy_stats);
}

bool RowGroup::CheckZonemap(ScanFilterInfo &filters) {
	auto &filter_list = filters.GetFilterList();
	// new row group - label all filters as up for grabs again
	filters.CheckAllFilters();
	for (idx_t i = 0; i < filter_list.size(); i++) {
		auto &entry = filter_list[i];
		auto &filter = entry.filter;
		auto base_column_index = entry.table_column_index;

		auto prune_result = GetColumn(base_column_index).CheckZonemap(filter);
		if (prune_result == FilterPropagateResult::FILTER_ALWAYS_FALSE) {
			return false;
		}
		if (filter.filter_type == TableFilterType::OPTIONAL_FILTER) {
			// these are only for row group checking, set as always true so we don't check it
			filters.SetFilterAlwaysTrue(i);
		} else if (prune_result == FilterPropagateResult::FILTER_ALWAYS_TRUE) {
			// filter is always true - no need to check it
			// label the filter as always true so we don't need to check it anymore
			filters.SetFilterAlwaysTrue(i);
		}
	}
	return true;
}

bool RowGroup::CheckZonemapSegments(CollectionScanState &state) {
	auto &filters = state.GetFilterInfo();
	for (auto &entry : filters.GetFilterList()) {
		if (entry.IsAlwaysTrue()) {
			// filter is always true - avoid checking
			continue;
		}
		auto column_idx = entry.scan_column_index;
		auto base_column_idx = entry.table_column_index;
		auto &filter = entry.filter;

		auto prune_result = GetColumn(base_column_idx).CheckZonemap(state.column_scans[column_idx], filter);
		if (prune_result != FilterPropagateResult::FILTER_ALWAYS_FALSE) {
			continue;
		}

		// check zone map segment.
		auto &column_scan_state = state.column_scans[column_idx];
		auto current_segment = column_scan_state.current;
		if (!current_segment) {
			// no segment to skip
			continue;
		}
		auto row_start = current_segment->row_start;
		idx_t target_row = row_start + current_segment->node->count;
		if (target_row >= state.max_row) {
			target_row = state.max_row;
		}
		D_ASSERT(target_row >= row_start);
		D_ASSERT(target_row <= row_start + this->count);
		idx_t target_vector_index = (target_row - row_start) / STANDARD_VECTOR_SIZE;
		if (state.vector_index == target_vector_index) {
			// we can't skip any full vectors because this segment contains less than a full vector
			// for now we just bail-out
			// FIXME: we could check if we can ALSO skip the next segments, in which case skipping a full vector
			// might be possible
			// we don't care that much though, since a single segment that fits less than a full vector is
			// exceedingly rare
			return true;
		}
		while (state.vector_index < target_vector_index) {
			NextVector(state);
		}
		return false;
	}

	return true;
}

template <TableScanType TYPE>
void RowGroup::TemplatedScan(TransactionData transaction, CollectionScanState &state, DataChunk &result) {
	const bool ALLOW_UPDATES = TYPE != TableScanType::TABLE_SCAN_COMMITTED_ROWS_DISALLOW_UPDATES &&
	                           TYPE != TableScanType::TABLE_SCAN_COMMITTED_ROWS_OMIT_PERMANENTLY_DELETED;
	const auto &column_ids = state.GetColumnIds();
	auto &filter_info = state.GetFilterInfo();
	while (true) {
		if (state.vector_index * STANDARD_VECTOR_SIZE >= state.max_row_group_row) {
			// exceeded the amount of rows to scan
			return;
		}
		idx_t current_row = state.vector_index * STANDARD_VECTOR_SIZE;
		auto max_count = MinValue<idx_t>(STANDARD_VECTOR_SIZE, state.max_row_group_row - current_row);

		// check the sampling info if we have to sample this chunk
		if (state.GetSamplingInfo().do_system_sample &&
		    state.random.NextRandom() > state.GetSamplingInfo().sample_rate) {
			NextVector(state);
			continue;
		}

		//! first check the zonemap if we have to scan this partition
		if (!CheckZonemapSegments(state)) {
			continue;
		}
		auto &current_row_group = *state.row_group->node;

		// second, scan the version chunk manager to figure out which tuples to load for this transaction
		idx_t count;
		if (TYPE == TableScanType::TABLE_SCAN_REGULAR) {
			count = current_row_group.GetSelVector(transaction, state.vector_index, state.valid_sel, max_count);
			if (count == 0) {
				// nothing to scan for this vector, skip the entire vector
				NextVector(state);
				continue;
			}
		} else if (TYPE == TableScanType::TABLE_SCAN_COMMITTED_ROWS_OMIT_PERMANENTLY_DELETED) {
			count = current_row_group.GetCommittedSelVector(transaction.start_time, transaction.transaction_id,
			                                                state.vector_index, state.valid_sel, max_count);
			if (count == 0) {
				// nothing to scan for this vector, skip the entire vector
				NextVector(state);
				continue;
			}
		} else {
			count = max_count;
		}
		auto &block_manager = GetBlockManager();
		if (block_manager.Prefetch()) {
			PrefetchState prefetch_state;
			for (idx_t i = 0; i < column_ids.size(); i++) {
				const auto &column = column_ids[i];
				GetColumn(column).InitializePrefetch(prefetch_state, state.column_scans[i], max_count);
			}
			auto &buffer_manager = block_manager.buffer_manager;
			buffer_manager.Prefetch(prefetch_state.blocks);
		}

		bool has_filters = filter_info.HasFilters();
		if (count == max_count && !has_filters) {
			// scan all vectors completely: full scan without deletions or table filters
			for (idx_t i = 0; i < column_ids.size(); i++) {
				const auto &column = column_ids[i];
				auto &col_data = GetColumn(column);
				if (TYPE != TableScanType::TABLE_SCAN_REGULAR) {
					col_data.ScanCommitted(state.vector_index, state.column_scans[i], result.data[i], ALLOW_UPDATES);
				} else {
					col_data.Scan(transaction, state.vector_index, state.column_scans[i], result.data[i]);
				}
			}
		} else {
			// partial scan: we have deletions or table filters
			idx_t approved_tuple_count = count;
			SelectionVector sel;
			if (count != max_count) {
				sel.Initialize(state.valid_sel);
			} else {
				sel.Initialize(nullptr);
			}
			//! first, we scan the columns with filters, fetch their data and generate a selection vector.
			//! get runtime statistics
			auto adaptive_filter = filter_info.GetAdaptiveFilter();
			auto filter_state = filter_info.BeginFilter();
			if (has_filters) {
				D_ASSERT(ALLOW_UPDATES);
				auto &filter_list = filter_info.GetFilterList();
				for (idx_t i = 0; i < filter_list.size(); i++) {
					auto filter_idx = adaptive_filter->permutation[i];
					auto &filter = filter_list[filter_idx];
					if (filter.IsAlwaysTrue()) {
						// this filter is always true - skip it
						continue;
					}
					auto &table_filter_state = *filter.filter_state;

					const auto scan_idx = filter.scan_column_index;
					const auto column_idx = filter.table_column_index;

					auto &result_vector = result.data[scan_idx];
					if (approved_tuple_count == 0) {
						auto &col_data = GetColumn(column_idx);
						col_data.Skip(state.column_scans[scan_idx]);
						continue;
					}
					auto &col_data = GetColumn(column_idx);
					col_data.Filter(transaction, state.vector_index, state.column_scans[scan_idx], result_vector, sel,
					                approved_tuple_count, filter.filter, table_filter_state);
				}
				for (auto &table_filter : filter_list) {
					if (table_filter.IsAlwaysTrue()) {
						continue;
					}
					result.data[table_filter.scan_column_index].Slice(sel, approved_tuple_count);
				}
			}
			if (approved_tuple_count == 0) {
				// all rows were filtered out by the table filters
				D_ASSERT(has_filters);
				result.Reset();
				// skip this vector in all the scans that were not scanned yet
				for (idx_t i = 0; i < column_ids.size(); i++) {
					auto &col_idx = column_ids[i];
					if (has_filters && filter_info.ColumnHasFilters(i)) {
						continue;
					}
					auto &col_data = GetColumn(col_idx);
					col_data.Skip(state.column_scans[i]);
				}
				state.vector_index++;
				continue;
			}
			//! Now we use the selection vector to fetch data for the other columns.
			for (idx_t i = 0; i < column_ids.size(); i++) {
				if (has_filters && filter_info.ColumnHasFilters(i)) {
					// column has already been scanned as part of the filtering process
					continue;
				}
				auto &column = column_ids[i];
				auto &col_data = GetColumn(column);
				if (TYPE == TableScanType::TABLE_SCAN_REGULAR) {
					col_data.Select(transaction, state.vector_index, state.column_scans[i], result.data[i], sel,
					                approved_tuple_count);
				} else {
					col_data.SelectCommitted(state.vector_index, state.column_scans[i], result.data[i], sel,
					                         approved_tuple_count, ALLOW_UPDATES);
				}
			}
			filter_info.EndFilter(filter_state);

			D_ASSERT(approved_tuple_count > 0);
			count = approved_tuple_count;
		}
		result.SetCardinality(count);
		state.vector_index++;
		break;
	}
}

void RowGroup::Scan(TransactionData transaction, CollectionScanState &state, DataChunk &result) {
	TemplatedScan<TableScanType::TABLE_SCAN_REGULAR>(transaction, state, result);
}

void RowGroup::ScanCommitted(CollectionScanState &state, DataChunk &result, TableScanType type) {
	auto &transaction_manager = DuckTransactionManager::Get(GetCollection().GetAttached());

	transaction_t start_ts;
	transaction_t transaction_id;
	if (type == TableScanType::TABLE_SCAN_LATEST_COMMITTED_ROWS) {
		start_ts = transaction_manager.GetLastCommit() + 1;
		transaction_id = MAX_TRANSACTION_ID;
	} else {
		start_ts = transaction_manager.LowestActiveStart();
		transaction_id = transaction_manager.LowestActiveId();
	}
	TransactionData data(transaction_id, start_ts);
	switch (type) {
	case TableScanType::TABLE_SCAN_COMMITTED_ROWS:
		TemplatedScan<TableScanType::TABLE_SCAN_COMMITTED_ROWS>(data, state, result);
		break;
	case TableScanType::TABLE_SCAN_COMMITTED_ROWS_DISALLOW_UPDATES:
		TemplatedScan<TableScanType::TABLE_SCAN_COMMITTED_ROWS_DISALLOW_UPDATES>(data, state, result);
		break;
	case TableScanType::TABLE_SCAN_COMMITTED_ROWS_OMIT_PERMANENTLY_DELETED:
	case TableScanType::TABLE_SCAN_LATEST_COMMITTED_ROWS:
		TemplatedScan<TableScanType::TABLE_SCAN_COMMITTED_ROWS_OMIT_PERMANENTLY_DELETED>(data, state, result);
		break;
	default:
		throw InternalException("Unrecognized table scan type");
	}
}

optional_ptr<RowVersionManager> RowGroup::GetVersionInfo() {
	if (!HasUnloadedDeletes()) {
		// deletes are loaded - return the version info
		return version_info;
	}
	lock_guard<mutex> lock(row_group_lock);
	// double-check after obtaining the lock whether or not deletes are still not loaded to avoid double load
	if (!HasUnloadedDeletes()) {
		return version_info;
	}
	// deletes are not loaded - reload
	auto root_delete = deletes_pointers[0];
	auto loaded_info = RowVersionManager::Deserialize(root_delete, GetBlockManager().GetMetadataManager());
	SetVersionInfo(std::move(loaded_info));
	deletes_is_loaded = true;
	return version_info;
}

void RowGroup::SetVersionInfo(shared_ptr<RowVersionManager> version) {
	owned_version_info = std::move(version);
	version_info = owned_version_info.get();
}

shared_ptr<RowVersionManager> RowGroup::GetOrCreateVersionInfoInternal() {
	// version info does not exist - need to create it
	lock_guard<mutex> lock(row_group_lock);
	if (!owned_version_info) {
		auto &buffer_manager = GetBlockManager().GetBufferManager();
		auto new_info = make_shared_ptr<RowVersionManager>(buffer_manager);
		SetVersionInfo(std::move(new_info));
	}
	return owned_version_info;
}

shared_ptr<RowVersionManager> RowGroup::GetOrCreateVersionInfoPtr() {
	auto vinfo = GetVersionInfo();
	if (vinfo) {
		// version info exists - return it directly
		return owned_version_info;
	}
	return GetOrCreateVersionInfoInternal();
}

RowVersionManager &RowGroup::GetOrCreateVersionInfo() {
	auto vinfo = GetVersionInfo();
	if (vinfo) {
		// version info exists - return it directly
		return *vinfo;
	}
	return *GetOrCreateVersionInfoInternal();
}

idx_t RowGroup::GetSelVector(TransactionData transaction, idx_t vector_idx, SelectionVector &sel_vector,
                             idx_t max_count) {
	auto vinfo = GetVersionInfo();
	if (!vinfo) {
		return max_count;
	}
	return vinfo->GetSelVector(transaction, vector_idx, sel_vector, max_count);
}

idx_t RowGroup::GetCommittedSelVector(transaction_t start_time, transaction_t transaction_id, idx_t vector_idx,
                                      SelectionVector &sel_vector, idx_t max_count) {
	auto vinfo = GetVersionInfo();
	if (!vinfo) {
		return max_count;
	}
	return vinfo->GetCommittedSelVector(start_time, transaction_id, vector_idx, sel_vector, max_count);
}

bool RowGroup::Fetch(TransactionData transaction, idx_t row) {
	if (UnsafeNumericCast<idx_t>(row) > count) {
		throw InternalException("RowGroup::Fetch - row_id out of range for row group");
	}
	auto vinfo = GetVersionInfo();
	if (!vinfo) {
		return true;
	}
	return vinfo->Fetch(transaction, row);
}

void RowGroup::FetchRow(TransactionData transaction, ColumnFetchState &state, const vector<StorageIndex> &column_ids,
                        row_t row_id, DataChunk &result, idx_t result_idx) {
	if (UnsafeNumericCast<idx_t>(row_id) > count) {
		throw InternalException("RowGroup::FetchRow - row_id out of range for row group");
	}
	for (idx_t col_idx = 0; col_idx < column_ids.size(); col_idx++) {
		auto &column = column_ids[col_idx];
		auto &result_vector = result.data[col_idx];
		D_ASSERT(result_vector.GetVectorType() == VectorType::FLAT_VECTOR);
		D_ASSERT(!FlatVector::IsNull(result_vector, result_idx));
		// regular column: fetch data from the base column
		auto &col_data = GetColumn(column);
		col_data.FetchRow(transaction, state, row_id, result_vector, result_idx);
	}
}

void RowGroup::SetCount(idx_t count) {
	this->count = count;
	if (!row_id_is_loaded) {
		lock_guard<mutex> guard(row_group_lock);
		if (!row_id_is_loaded) {
			return;
		}
	}
	row_id_column_data->count = count;
}

void RowGroup::AppendVersionInfo(TransactionData transaction, idx_t count) {
	const idx_t row_group_size = GetRowGroupSize();
	idx_t row_group_start = this->count.load();
	idx_t row_group_end = row_group_start + count;
	if (row_group_end > row_group_size) {
		row_group_end = row_group_size;
	}
	// create the version_info if it doesn't exist yet
	auto &vinfo = GetOrCreateVersionInfo();
	vinfo.AppendVersionInfo(transaction, count, row_group_start, row_group_end);
	SetCount(row_group_end);
}

void RowGroup::CommitAppend(transaction_t commit_id, idx_t row_group_start, idx_t count) {
	auto &vinfo = GetOrCreateVersionInfo();
	vinfo.CommitAppend(commit_id, row_group_start, count);
}

void RowGroup::RevertAppend(idx_t new_count) {
	if (new_count > this->count) {
		throw InternalException("RowGroup::RevertAppend new_count out of range");
	}
	auto &vinfo = GetOrCreateVersionInfo();
	vinfo.RevertAppend(new_count);
	for (auto &column : GetColumns()) {
		column->RevertAppend(UnsafeNumericCast<row_t>(new_count));
	}
	SetCount(new_count);
	Verify();
}

void RowGroup::InitializeAppend(RowGroupAppendState &append_state) {
	append_state.row_group = this;
	append_state.offset_in_row_group = this->count;
	// for each column, initialize the append state
	append_state.states = make_unsafe_uniq_array<ColumnAppendState>(GetColumnCount());
	for (idx_t i = 0; i < GetColumnCount(); i++) {
		auto &col_data = GetColumn(i);
		col_data.InitializeAppend(append_state.states[i]);
	}
}

void RowGroup::Append(RowGroupAppendState &state, DataChunk &chunk, idx_t append_count) {
	// append to the current row_group
	D_ASSERT(chunk.ColumnCount() == GetColumnCount());
	for (idx_t i = 0; i < GetColumnCount(); i++) {
		auto &col_data = GetColumn(i);
		auto prev_allocation_size = col_data.GetAllocationSize();
		col_data.Append(state.states[i], chunk.data[i], append_count);
		allocation_size += col_data.GetAllocationSize() - prev_allocation_size;
	}
	state.offset_in_row_group += append_count;
}

void RowGroup::CleanupAppend(transaction_t lowest_transaction, idx_t start, idx_t count) {
	auto &vinfo = GetOrCreateVersionInfo();
	vinfo.CleanupAppend(lowest_transaction, start, count);
}

void RowGroup::Update(TransactionData transaction, DataTable &data_table, DataChunk &update_chunk, row_t *ids,
                      idx_t offset, idx_t count, const vector<PhysicalIndex> &column_ids, idx_t row_group_start) {
#ifdef DEBUG
	for (size_t i = offset; i < offset + count; i++) {
		D_ASSERT(ids[i] >= row_t(row_group_start) && ids[i] < row_t(row_group_start + this->count));
	}
#endif
	for (idx_t i = 0; i < column_ids.size(); i++) {
		auto column = column_ids[i];
		auto &col_data = GetColumn(column.index);
		D_ASSERT(col_data.type.id() == update_chunk.data[i].GetType().id());
		if (offset > 0) {
			Vector sliced_vector(update_chunk.data[i], offset, offset + count);
			sliced_vector.Flatten(count);
			col_data.Update(transaction, data_table, column.index, sliced_vector, ids + offset, count, row_group_start);
		} else {
			col_data.Update(transaction, data_table, column.index, update_chunk.data[i], ids, count, row_group_start);
		}
		MergeStatistics(column.index, *col_data.GetUpdateStatistics());
	}
}

void RowGroup::UpdateColumn(TransactionData transaction, DataTable &data_table, DataChunk &updates, Vector &row_ids,
                            idx_t offset, idx_t count, const vector<column_t> &column_path, idx_t row_group_start) {
	D_ASSERT(updates.ColumnCount() == 1);
	auto ids = FlatVector::GetData<row_t>(row_ids);

	auto primary_column_idx = column_path[0];
	D_ASSERT(primary_column_idx < columns.size());
	auto &col_data = GetColumn(primary_column_idx);
	idx_t depth = 1;
	if (offset > 0) {
		Vector sliced_vector(updates.data[0], offset, offset + count);
		sliced_vector.Flatten(count);
		col_data.UpdateColumn(transaction, data_table, column_path, sliced_vector, ids + offset, count, depth,
		                      row_group_start);
	} else {
		col_data.UpdateColumn(transaction, data_table, column_path, updates.data[0], ids, count, depth,
		                      row_group_start);
	}
	MergeStatistics(primary_column_idx, *col_data.GetUpdateStatistics());
}

unique_ptr<BaseStatistics> RowGroup::GetStatistics(idx_t column_idx) {
	auto &col_data = GetColumn(column_idx);
	return col_data.GetStatistics();
}

void RowGroup::MergeStatistics(idx_t column_idx, const BaseStatistics &other) {
	auto &col_data = GetColumn(column_idx);
	col_data.MergeStatistics(other);
}

void RowGroup::MergeIntoStatistics(idx_t column_idx, BaseStatistics &other) {
	auto &col_data = GetColumn(column_idx);
	col_data.MergeIntoStatistics(other);
}

void RowGroup::MergeIntoStatistics(TableStatistics &other) {
	auto stats_lock = other.GetLock();
	for (idx_t i = 0; i < columns.size(); i++) {
		MergeIntoStatistics(i, other.GetStats(*stats_lock, i).Statistics());
	}
}

ColumnCheckpointInfo::ColumnCheckpointInfo(RowGroupWriteInfo &info, idx_t column_idx)
    : column_idx(column_idx), info(info) {
}

RowGroupWriteInfo::RowGroupWriteInfo(PartialBlockManager &manager, const vector<CompressionType> &compression_types,
                                     CheckpointType checkpoint_type)
    : manager(manager), compression_types(compression_types), checkpoint_type(checkpoint_type) {
}

RowGroupWriteInfo::RowGroupWriteInfo(PartialBlockManager &manager, const vector<CompressionType> &compression_types,
                                     vector<unique_ptr<PartialBlockManager>> &column_partial_block_managers_p)
    : manager(manager), compression_types(compression_types), checkpoint_type(CheckpointType::FULL_CHECKPOINT),
      column_partial_block_managers(column_partial_block_managers_p) {
}

PartialBlockManager &RowGroupWriteInfo::GetPartialBlockManager(idx_t column_idx) {
	if (column_partial_block_managers && !column_partial_block_managers->empty()) {
		return *column_partial_block_managers->at(column_idx);
	}
	return manager;
}

PartialBlockManager &ColumnCheckpointInfo::GetPartialBlockManager() {
	return info.GetPartialBlockManager(column_idx);
}

CompressionType ColumnCheckpointInfo::GetCompressionType() {
	return info.compression_types[column_idx];
}

vector<RowGroupWriteData> RowGroup::WriteToDisk(RowGroupWriteInfo &info,
                                                const vector<const_reference<RowGroup>> &row_groups) {
	vector<RowGroupWriteData> result;
	if (row_groups.empty()) {
		return result;
	}

	idx_t column_count = row_groups[0].get().GetColumnCount();
	for (auto &row_group : row_groups) {
		D_ASSERT(column_count == row_group.get().GetColumnCount());
		RowGroupWriteData write_data;
		write_data.states.reserve(column_count);
		write_data.statistics.reserve(column_count);
		result.push_back(std::move(write_data));
	}

	// Checkpoint the row groups
	// In order to co-locate columns across different row groups, we write column-at-a-time
	// i.e. we first write column #0 of all row groups, then column #1, ...

	// Each column can have multiple segments.
	// (Some columns will be wider than others, and require different numbers
	// of blocks to encode.) Segments cannot span blocks.
	//
	// Some of these columns are composite (list, struct). The data is written
	// first sequentially, and the pointers are written later, so that the
	// pointers all end up densely packed, and thus more cache-friendly.
	vector<vector<shared_ptr<ColumnData>>> result_columns;
	result_columns.resize(row_groups.size());
	for (idx_t column_idx = 0; column_idx < column_count; column_idx++) {
		for (idx_t row_group_idx = 0; row_group_idx < row_groups.size(); row_group_idx++) {
			auto &row_group = row_groups[row_group_idx].get();
			auto &row_group_write_data = result[row_group_idx];
			auto &column = row_group.GetColumn(column_idx);
			ColumnCheckpointInfo checkpoint_info(info, column_idx);
			auto checkpoint_state = column.Checkpoint(row_group, checkpoint_info);

			auto result_col = checkpoint_state->GetFinalResult();
			// FIXME: we should get rid of the checkpoint state statistics - and instead use the stats in the ColumnData
			// directly
			auto stats = checkpoint_state->GetStatistics();
			result_col->MergeStatistics(*stats);

			result_columns[row_group_idx].push_back(std::move(result_col));
			row_group_write_data.statistics.push_back(stats->Copy());
			row_group_write_data.states.push_back(std::move(checkpoint_state));
		}
	}

	// create the row groups
	for (idx_t row_group_idx = 0; row_group_idx < row_groups.size(); row_group_idx++) {
		auto &row_group_write_data = result[row_group_idx];
		auto &row_group = row_groups[row_group_idx].get();
		auto result_row_group = make_shared_ptr<RowGroup>(row_group.GetCollection(), row_group.count);
		result_row_group->columns = std::move(result_columns[row_group_idx]);
		result_row_group->version_info = row_group.version_info.load();
		result_row_group->owned_version_info = row_group.owned_version_info;

		row_group_write_data.result_row_group = std::move(result_row_group);
	}

	return result;
}

RowGroupWriteData RowGroup::WriteToDisk(RowGroupWriteInfo &info) const {
	vector<const_reference<RowGroup>> row_groups;
	row_groups.push_back(*this);
	auto result = WriteToDisk(info, row_groups);
	return std::move(result[0]);
}

idx_t RowGroup::GetCommittedRowCount() {
	auto vinfo = GetVersionInfo();
	if (!vinfo) {
		return count;
	}
	return count - vinfo->GetCommittedDeletedCount(count);
}

bool RowGroup::HasUnloadedDeletes() const {
	if (deletes_pointers.empty()) {
		// no stored deletes at all
		return false;
	}
	// return whether or not the deletes have been loaded
	return !deletes_is_loaded;
}

vector<idx_t> RowGroup::GetOrComputeExtraMetadataBlocks(bool force_compute) {
	if (has_metadata_blocks && !force_compute) {
		return extra_metadata_blocks;
	}
	if (column_pointers.empty()) {
		// no pointers
		return {};
	}
	vector<MetaBlockPointer> read_pointers;
	// column_pointers stores the beginning of each column
	// if columns are big - they may span multiple metadata blocks
	// we need to figure out all blocks that this row group points to
	// we need to follow the linked list in the metadata blocks to allow for this
	auto &metadata_manager = GetCollection().GetMetadataManager();
	idx_t last_idx = column_pointers.size() - 1;
	if (column_pointers.size() > 1) {
		// for all but the last column pointer - we can just follow the linked list until we reach the last column
		MetadataReader reader(metadata_manager, column_pointers[0]);
		auto last_pointer = column_pointers[last_idx];
		read_pointers = reader.GetRemainingBlocks(last_pointer);
	}
	// for the last column we need to deserialize the column - because we don't know where it stops
	auto &types = GetCollection().GetTypes();
	MetadataReader reader(metadata_manager, column_pointers[last_idx], &read_pointers);
	ColumnData::Deserialize(GetBlockManager(), GetTableInfo(), last_idx, reader, types[last_idx]);

	unordered_set<idx_t> result_as_set;
	for (auto &ptr : read_pointers) {
		result_as_set.emplace(ptr.block_pointer);
	}
	for (auto &ptr : column_pointers) {
		result_as_set.erase(ptr.block_pointer);
	}
	return {result_as_set.begin(), result_as_set.end()};
}

const vector<MetaBlockPointer> &RowGroup::GetColumnStartPointers() const {
	return column_pointers;
}

RowGroupWriteData RowGroup::WriteToDisk(RowGroupWriter &writer) {
	if (DBConfig::GetSetting<ExperimentalMetadataReuseSetting>(writer.GetDatabase()) && !column_pointers.empty() &&
	    !HasChanges()) {
		// we have existing metadata and the row group has not been changed
		// re-use previous metadata
		RowGroupWriteData result;
		result.reuse_existing_metadata_blocks = true;
		result.existing_extra_metadata_blocks = GetOrComputeExtraMetadataBlocks();
		return result;
	}
	auto &compression_types = writer.GetCompressionTypes();
	if (columns.size() != compression_types.size()) {
		throw InternalException("RowGroup::WriteToDisk - mismatch in column count vs compression types");
	}
	for (idx_t column_idx = 0; column_idx < GetColumnCount(); column_idx++) {
		auto &column = GetColumn(column_idx);
		if (column.count != this->count) {
			throw InternalException("Corrupted in-memory column - column with index %llu has misaligned count (row "
			                        "group has %llu rows, column has %llu)",
			                        column_idx, this->count.load(), column.count.load());
		}
	}

	RowGroupWriteInfo info(writer.GetPartialBlockManager(), compression_types, writer.GetCheckpointType());
	return WriteToDisk(info);
}

void IncrementSegmentStart(PersistentColumnData &data, idx_t start_increment) {
	for (auto &pointer : data.pointers) {
		pointer.row_start += start_increment;
	}
	for (auto &child_column : data.child_columns) {
		IncrementSegmentStart(child_column, start_increment);
	}
}

RowGroupPointer RowGroup::Checkpoint(RowGroupWriteData write_data, RowGroupWriter &writer,
                                     TableStatistics &global_stats, idx_t row_group_start) {
	RowGroupPointer row_group_pointer;

	auto metadata_manager = writer.GetMetadataManager();
	// construct the row group pointer and write the column meta data to disk
	row_group_pointer.row_start = row_group_start;
	row_group_pointer.tuple_count = count;
	if (write_data.reuse_existing_metadata_blocks) {
		// we are re-using the previous metadata
		row_group_pointer.data_pointers = column_pointers;
		row_group_pointer.has_metadata_blocks = true;
		row_group_pointer.extra_metadata_blocks = write_data.existing_extra_metadata_blocks;
		row_group_pointer.deletes_pointers = deletes_pointers;
		if (metadata_manager) {
			vector<MetaBlockPointer> extra_metadata_block_pointers;
			extra_metadata_block_pointers.reserve(write_data.existing_extra_metadata_blocks.size());
			for (auto &block_pointer : write_data.existing_extra_metadata_blocks) {
				extra_metadata_block_pointers.emplace_back(block_pointer, 0);
			}
			metadata_manager->ClearModifiedBlocks(column_pointers);
			metadata_manager->ClearModifiedBlocks(extra_metadata_block_pointers);
			metadata_manager->ClearModifiedBlocks(deletes_pointers);

			// remember metadata_blocks to avoid loading them on future checkpoints
			has_metadata_blocks = true;
			extra_metadata_blocks = row_group_pointer.extra_metadata_blocks;
		}
		return row_group_pointer;
	}
	D_ASSERT(write_data.states.size() == columns.size());
	{
		auto lock = global_stats.GetLock();
		for (idx_t column_idx = 0; column_idx < GetColumnCount(); column_idx++) {
			global_stats.GetStats(*lock, column_idx).Statistics().Merge(write_data.statistics[column_idx]);
		}
	}
	vector<MetaBlockPointer> column_metadata;
	unordered_set<idx_t> metadata_blocks;
	writer.StartWritingColumns(column_metadata);
	for (auto &state : write_data.states) {
		// get the current position of the table data writer
		auto &data_writer = writer.GetPayloadWriter();
		auto pointer = writer.GetMetaBlockPointer();

		// store the stats and the data pointers in the row group pointers
		row_group_pointer.data_pointers.push_back(pointer);
		metadata_blocks.insert(pointer.block_pointer);

		// Write pointers to the column segments.
		//
		// Just as above, the state can refer to many other states, so this
		// can cascade recursively into more pointer writes.
		auto persistent_data = state->ToPersistentData();
		// increment the "start" in all data pointers by the row group start
		// FIXME: this is only necessary when targeting old serialization
		IncrementSegmentStart(persistent_data, row_group_start);
		BinarySerializer serializer(data_writer);
		serializer.Begin();
		persistent_data.Serialize(serializer);
		serializer.End();
	}
	writer.FinishWritingColumns();

	row_group_pointer.has_metadata_blocks = true;
	for (auto &column_pointer : column_metadata) {
		auto entry = metadata_blocks.find(column_pointer.block_pointer);
		if (entry != metadata_blocks.end()) {
			// this metadata block is already stored in "data_pointers" - no need to duplicate it
			continue;
		}
		// this metadata block is not stored - add it to the extra metadata blocks
		row_group_pointer.extra_metadata_blocks.push_back(column_pointer.block_pointer);
		metadata_blocks.insert(column_pointer.block_pointer);
	}
	// set up the pointers correctly within this row group for future operations
	column_pointers = row_group_pointer.data_pointers;
	has_metadata_blocks = true;
	extra_metadata_blocks = row_group_pointer.extra_metadata_blocks;

	if (metadata_manager) {
		row_group_pointer.deletes_pointers = CheckpointDeletes(*metadata_manager);
	}
	Verify();
	return row_group_pointer;
}

bool RowGroup::HasChanges() const {
	if (has_changes) {
		return true;
	}
	if (version_info.load()) {
		// we have deletes
		return true;
	}
	D_ASSERT(!deletes_is_loaded.load());
	// check if any of the columns have changes
	// avoid loading unloaded columns - unloaded columns can never have changes
	for (idx_t c = 0; c < columns.size(); c++) {
		if (is_loaded && !is_loaded[c]) {
			continue;
		}
		if (columns[c]->HasAnyChanges()) {
			return true;
		}
	}
	return false;
}

bool RowGroup::IsPersistent() const {
	for (auto &column : columns) {
		if (!column->IsPersistent()) {
			// column is not persistent
			return false;
		}
	}
	return true;
}

PersistentRowGroupData RowGroup::SerializeRowGroupInfo(idx_t row_group_start) const {
	// all columns are persistent - serialize
	PersistentRowGroupData result;
	for (auto &col : columns) {
		result.column_data.push_back(col->Serialize());
	}
	result.start = row_group_start;
	result.count = count;
	return result;
}

vector<MetaBlockPointer> RowGroup::CheckpointDeletes(MetadataManager &manager) {
	if (HasUnloadedDeletes()) {
		// deletes were not loaded so they cannot be changed
		// re-use them as-is
		manager.ClearModifiedBlocks(deletes_pointers);
		return deletes_pointers;
	}
	auto vinfo = GetVersionInfo();
	if (!vinfo) {
		// no version information: write nothing
		return vector<MetaBlockPointer>();
	}
	return vinfo->Checkpoint(manager);
}

void RowGroup::Serialize(RowGroupPointer &pointer, Serializer &serializer) {
	serializer.WriteProperty(100, "row_start", pointer.row_start);
	serializer.WriteProperty(101, "tuple_count", pointer.tuple_count);
	serializer.WriteProperty(102, "data_pointers", pointer.data_pointers);
	serializer.WriteProperty(103, "delete_pointers", pointer.deletes_pointers);
	if (serializer.ShouldSerialize(6)) {
		serializer.WriteProperty(104, "has_metadata_blocks", pointer.has_metadata_blocks);
		serializer.WritePropertyWithDefault(105, "extra_metadata_blocks", pointer.extra_metadata_blocks);
	}
}

RowGroupPointer RowGroup::Deserialize(Deserializer &deserializer) {
	RowGroupPointer result;
	result.row_start = deserializer.ReadProperty<uint64_t>(100, "row_start");
	result.tuple_count = deserializer.ReadProperty<uint64_t>(101, "tuple_count");
	result.data_pointers = deserializer.ReadProperty<vector<MetaBlockPointer>>(102, "data_pointers");
	result.deletes_pointers = deserializer.ReadProperty<vector<MetaBlockPointer>>(103, "delete_pointers");
	result.has_metadata_blocks = deserializer.ReadPropertyWithExplicitDefault<bool>(104, "has_metadata_blocks", false);
	result.extra_metadata_blocks = deserializer.ReadPropertyWithDefault<vector<idx_t>>(105, "extra_metadata_blocks");
	return result;
}

//===--------------------------------------------------------------------===//
// GetPartitionStats
//===--------------------------------------------------------------------===//
PartitionStatistics RowGroup::GetPartitionStats(idx_t row_group_start) const {
	PartitionStatistics result;
	result.row_start = row_group_start;
	result.count = count;
	if (HasUnloadedDeletes() || version_info.load().get()) {
		// we have version info - approx count
		result.count_type = CountType::COUNT_APPROXIMATE;
	} else {
		result.count_type = CountType::COUNT_EXACT;
	}
	return result;
}

//===--------------------------------------------------------------------===//
// GetColumnSegmentInfo
//===--------------------------------------------------------------------===//
void RowGroup::GetColumnSegmentInfo(const QueryContext &context, idx_t row_group_index,
                                    vector<ColumnSegmentInfo> &result) {
	for (idx_t col_idx = 0; col_idx < GetColumnCount(); col_idx++) {
		auto &col_data = GetColumn(col_idx);
		col_data.GetColumnSegmentInfo(context, row_group_index, {col_idx}, result);
	}
}

//===--------------------------------------------------------------------===//
// Version Delete Information
//===--------------------------------------------------------------------===//
class VersionDeleteState {
public:
	VersionDeleteState(RowGroup &info, TransactionData transaction, DataTable &table, idx_t base_row)
	    : info(info), transaction(transaction), table(table), current_chunk(DConstants::INVALID_INDEX), count(0),
	      base_row(base_row), delete_count(0) {
	}

	RowGroup &info;
	TransactionData transaction;
	DataTable &table;
	idx_t current_chunk;
	row_t rows[STANDARD_VECTOR_SIZE];
	idx_t count;
	idx_t base_row;
	idx_t chunk_row;
	idx_t delete_count;

public:
	void Delete(row_t row_id);
	void Flush();
};

idx_t RowGroup::Delete(TransactionData transaction, DataTable &table, row_t *ids, idx_t count, idx_t row_group_start) {
	VersionDeleteState del_state(*this, transaction, table, row_group_start);

	// obtain a write lock
	for (idx_t i = 0; i < count; i++) {
		D_ASSERT(ids[i] >= 0);
		D_ASSERT(idx_t(ids[i]) >= row_group_start && idx_t(ids[i]) < row_group_start + this->count);
		del_state.Delete(ids[i] - UnsafeNumericCast<row_t>(row_group_start));
	}
	del_state.Flush();
	return del_state.delete_count;
}

void RowGroup::Verify() {
#ifdef DEBUG
	for (auto &column : GetColumns()) {
		column->Verify(*this);
	}
	lock_guard<mutex> guard(row_group_lock);
	if (row_id_is_loaded) {
		D_ASSERT(row_id_column_data->count == count);
	}
#endif
}

idx_t RowGroup::DeleteRows(idx_t vector_idx, transaction_t transaction_id, row_t rows[], idx_t count) {
	return GetOrCreateVersionInfo().DeleteRows(vector_idx, transaction_id, rows, count);
}

void VersionDeleteState::Delete(row_t row_id) {
	D_ASSERT(row_id >= 0);
	idx_t vector_idx = UnsafeNumericCast<idx_t>(row_id) / STANDARD_VECTOR_SIZE;
	idx_t idx_in_vector = UnsafeNumericCast<idx_t>(row_id) - vector_idx * STANDARD_VECTOR_SIZE;
	if (current_chunk != vector_idx) {
		Flush();

		current_chunk = vector_idx;
		chunk_row = vector_idx * STANDARD_VECTOR_SIZE;
	}
	rows[count++] = UnsafeNumericCast<row_t>(idx_in_vector);
}

void VersionDeleteState::Flush() {
	if (count == 0) {
		return;
	}
	// it is possible for delete statements to delete the same tuple multiple times when combined with a USING clause
	// in the current_info->Delete, we check which tuples are actually deleted (excluding duplicate deletions)
	// this is returned in the actual_delete_count
	auto actual_delete_count = info.DeleteRows(current_chunk, transaction.transaction_id, rows, count);
	delete_count += actual_delete_count;
	if (transaction.transaction && actual_delete_count > 0) {
		// now push the delete into the undo buffer, but only if any deletes were actually performed
		transaction.transaction->PushDelete(table, info.GetOrCreateVersionInfo(), current_chunk, rows,
		                                    actual_delete_count, base_row + chunk_row);
	}
	count = 0;
}

} // namespace duckdb
