#include "duckdb/storage/table/row_group.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/storage/table/column_data.hpp"
#include "duckdb/storage/table/column_checkpoint_state.hpp"
#include "duckdb/storage/table/update_segment.hpp"
#include "duckdb/storage/table_storage_info.hpp"
#include "duckdb/common/chrono.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/storage/checkpoint/table_data_writer.hpp"
#include "duckdb/storage/metadata/metadata_reader.hpp"
#include "duckdb/transaction/duck_transaction_manager.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/transaction/duck_transaction.hpp"
#include "duckdb/storage/table/append_state.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/storage/table/row_version_manager.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/serializer/binary_serializer.hpp"

namespace duckdb {

RowGroup::RowGroup(RowGroupCollection &collection, idx_t start, idx_t count)
    : SegmentBase<RowGroup>(start, count), collection(collection) {
	Verify();
}

RowGroup::RowGroup(RowGroupCollection &collection, RowGroupPointer &&pointer)
    : SegmentBase<RowGroup>(pointer.row_start, pointer.tuple_count), collection(collection) {
	// deserialize the columns
	if (pointer.data_pointers.size() != collection.GetTypes().size()) {
		throw IOException("Row group column count is unaligned with table column count. Corrupt file?");
	}
	this->column_pointers = std::move(pointer.data_pointers);
	this->columns.resize(column_pointers.size());
	this->is_loaded = unique_ptr<atomic<bool>[]>(new atomic<bool>[columns.size()]);
	for (idx_t c = 0; c < columns.size(); c++) {
		this->is_loaded[c] = false;
	}
	this->deletes_pointers = std::move(pointer.deletes_pointers);
	this->deletes_is_loaded = false;

	Verify();
}

void RowGroup::MoveToCollection(RowGroupCollection &collection, idx_t new_start) {
	this->collection = collection;
	this->start = new_start;
	for (auto &column : GetColumns()) {
		column->SetStart(new_start);
	}
	if (!HasUnloadedDeletes()) {
		auto &vinfo = GetVersionInfo();
		if (vinfo) {
			vinfo->SetStart(new_start);
		}
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

ColumnData &RowGroup::GetColumn(storage_t c) {
	D_ASSERT(c < columns.size());
	if (!is_loaded) {
		// not being lazy loaded
		D_ASSERT(columns[c]);
		return *columns[c];
	}
	if (is_loaded[c]) {
		D_ASSERT(columns[c]);
		return *columns[c];
	}
	lock_guard<mutex> l(row_group_lock);
	if (columns[c]) {
		D_ASSERT(is_loaded[c]);
		return *columns[c];
	}
	if (column_pointers.size() != columns.size()) {
		throw InternalException("Lazy loading a column but the pointer was not set");
	}
	auto &metadata_manager = GetCollection().GetMetadataManager();
	auto &types = GetCollection().GetTypes();
	auto &block_pointer = column_pointers[c];
	MetadataReader column_data_reader(metadata_manager, block_pointer);
	this->columns[c] =
	    ColumnData::Deserialize(GetBlockManager(), GetTableInfo(), c, start, column_data_reader, types[c], nullptr);
	is_loaded[c] = true;
	if (this->columns[c]->count != this->count) {
		throw InternalException("Corrupted database - loaded column with index %llu at row start %llu, count %llu did "
		                        "not match count of row group %llu",
		                        c, start, this->columns[c]->count, this->count.load());
	}
	return *columns[c];
}

BlockManager &RowGroup::GetBlockManager() {
	return GetCollection().GetBlockManager();
}
DataTableInfo &RowGroup::GetTableInfo() {
	return GetCollection().GetTableInfo();
}

void RowGroup::InitializeEmpty(const vector<LogicalType> &types) {
	// set up the segment trees for the column segments
	D_ASSERT(columns.empty());
	for (idx_t i = 0; i < types.size(); i++) {
		auto column_data = ColumnData::CreateColumn(GetBlockManager(), GetTableInfo(), i, start, types[i]);
		columns.push_back(std::move(column_data));
	}
}

void ColumnScanState::Initialize(const LogicalType &type) {
	if (type.id() == LogicalTypeId::VALIDITY) {
		// validity - nothing to initialize
		return;
	}
	if (type.InternalType() == PhysicalType::STRUCT) {
		// validity + struct children
		auto &struct_children = StructType::GetChildTypes(type);
		child_states.resize(struct_children.size() + 1);
		for (idx_t i = 0; i < struct_children.size(); i++) {
			child_states[i + 1].Initialize(struct_children[i].second);
		}
	} else if (type.InternalType() == PhysicalType::LIST) {
		// validity + list child
		child_states.resize(2);
		child_states[1].Initialize(ListType::GetChildType(type));
	} else {
		// validity
		child_states.resize(1);
	}
}

void CollectionScanState::Initialize(const vector<LogicalType> &types) {
	auto &column_ids = GetColumnIds();
	column_scans = make_unsafe_uniq_array<ColumnScanState>(column_ids.size());
	for (idx_t i = 0; i < column_ids.size(); i++) {
		if (column_ids[i] == COLUMN_IDENTIFIER_ROW_ID) {
			continue;
		}
		column_scans[i].Initialize(types[column_ids[i]]);
	}
}

bool RowGroup::InitializeScanWithOffset(CollectionScanState &state, idx_t vector_offset) {
	auto &column_ids = state.GetColumnIds();
	auto filters = state.GetFilters();
	if (filters) {
		if (!CheckZonemap(*filters, column_ids)) {
			return false;
		}
	}

	state.row_group = this;
	state.vector_index = vector_offset;
	state.max_row_group_row =
	    this->start > state.max_row ? 0 : MinValue<idx_t>(this->count, state.max_row - this->start);
	D_ASSERT(state.column_scans);
	for (idx_t i = 0; i < column_ids.size(); i++) {
		const auto &column = column_ids[i];
		if (column != COLUMN_IDENTIFIER_ROW_ID) {
			auto &column_data = GetColumn(column);
			column_data.InitializeScanWithOffset(state.column_scans[i], start + vector_offset * STANDARD_VECTOR_SIZE);
		} else {
			state.column_scans[i].current = nullptr;
		}
	}
	return true;
}

bool RowGroup::InitializeScan(CollectionScanState &state) {
	auto &column_ids = state.GetColumnIds();
	auto filters = state.GetFilters();
	if (filters) {
		if (!CheckZonemap(*filters, column_ids)) {
			return false;
		}
	}
	state.row_group = this;
	state.vector_index = 0;
	state.max_row_group_row =
	    this->start > state.max_row ? 0 : MinValue<idx_t>(this->count, state.max_row - this->start);
	if (state.max_row_group_row == 0) {
		return false;
	}
	D_ASSERT(state.column_scans);
	for (idx_t i = 0; i < column_ids.size(); i++) {
		auto column = column_ids[i];
		if (column != COLUMN_IDENTIFIER_ROW_ID) {
			auto &column_data = GetColumn(column);
			column_data.InitializeScan(state.column_scans[i]);
		} else {
			state.column_scans[i].current = nullptr;
		}
	}
	return true;
}

unique_ptr<RowGroup> RowGroup::AlterType(RowGroupCollection &new_collection, const LogicalType &target_type,
                                         idx_t changed_idx, ExpressionExecutor &executor,
                                         CollectionScanState &scan_state, DataChunk &scan_chunk) {
	Verify();

	// construct a new column data for this type
	auto column_data = ColumnData::CreateColumn(GetBlockManager(), GetTableInfo(), changed_idx, start, target_type);

	ColumnAppendState append_state;
	column_data->InitializeAppend(append_state);

	// scan the original table, and fill the new column with the transformed value
	scan_state.Initialize(GetCollection().GetTypes());
	InitializeScan(scan_state);

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
	auto row_group = make_uniq<RowGroup>(new_collection, this->start, this->count);
	row_group->version_info = GetOrCreateVersionInfoPtr();
	auto &cols = GetColumns();
	for (idx_t i = 0; i < cols.size(); i++) {
		if (i == changed_idx) {
			// this is the altered column: use the new column
			row_group->columns.push_back(std::move(column_data));
		} else {
			// this column was not altered: use the data directly
			row_group->columns.push_back(cols[i]);
		}
	}
	row_group->Verify();
	return row_group;
}

unique_ptr<RowGroup> RowGroup::AddColumn(RowGroupCollection &new_collection, ColumnDefinition &new_column,
                                         ExpressionExecutor &executor, Expression &default_value, Vector &result) {
	Verify();

	// construct a new column data for the new column
	auto added_column =
	    ColumnData::CreateColumn(GetBlockManager(), GetTableInfo(), GetColumnCount(), start, new_column.Type());

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
	auto row_group = make_uniq<RowGroup>(new_collection, this->start, this->count);
	row_group->version_info = GetOrCreateVersionInfoPtr();
	row_group->columns = GetColumns();
	// now add the new column
	row_group->columns.push_back(std::move(added_column));

	row_group->Verify();
	return row_group;
}

unique_ptr<RowGroup> RowGroup::RemoveColumn(RowGroupCollection &new_collection, idx_t removed_column) {
	Verify();

	D_ASSERT(removed_column < columns.size());

	auto row_group = make_uniq<RowGroup>(new_collection, this->start, this->count);
	row_group->version_info = GetOrCreateVersionInfoPtr();
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

void RowGroup::CommitDropColumn(idx_t column_idx) {
	GetColumn(column_idx).CommitDropColumn();
}

void RowGroup::NextVector(CollectionScanState &state) {
	state.vector_index++;
	const auto &column_ids = state.GetColumnIds();
	for (idx_t i = 0; i < column_ids.size(); i++) {
		const auto &column = column_ids[i];
		if (column == COLUMN_IDENTIFIER_ROW_ID) {
			continue;
		}
		D_ASSERT(column < columns.size());
		GetColumn(column).Skip(state.column_scans[i]);
	}
}

bool RowGroup::CheckZonemap(TableFilterSet &filters, const vector<storage_t> &column_ids) {
	for (auto &entry : filters.filters) {
		auto column_index = entry.first;
		auto &filter = entry.second;
		const auto &base_column_index = column_ids[column_index];
		if (!GetColumn(base_column_index).CheckZonemap(*filter)) {
			return false;
		}
	}
	return true;
}

bool RowGroup::CheckZonemapSegments(CollectionScanState &state) {
	auto &column_ids = state.GetColumnIds();
	auto filters = state.GetFilters();
	if (!filters) {
		return true;
	}
	for (auto &entry : filters->filters) {
		D_ASSERT(entry.first < column_ids.size());
		auto column_idx = entry.first;
		const auto &base_column_idx = column_ids[column_idx];
		bool read_segment = GetColumn(base_column_idx).CheckZonemap(state.column_scans[column_idx], *entry.second);
		if (!read_segment) {
			idx_t target_row =
			    state.column_scans[column_idx].current->start + state.column_scans[column_idx].current->count;
			D_ASSERT(target_row >= this->start);
			D_ASSERT(target_row <= this->start + this->count);
			idx_t target_vector_index = (target_row - this->start) / STANDARD_VECTOR_SIZE;
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
	}

	return true;
}

template <TableScanType TYPE>
void RowGroup::TemplatedScan(TransactionData transaction, CollectionScanState &state, DataChunk &result) {
	const bool ALLOW_UPDATES = TYPE != TableScanType::TABLE_SCAN_COMMITTED_ROWS_DISALLOW_UPDATES &&
	                           TYPE != TableScanType::TABLE_SCAN_COMMITTED_ROWS_OMIT_PERMANENTLY_DELETED;
	auto table_filters = state.GetFilters();
	const auto &column_ids = state.GetColumnIds();
	auto adaptive_filter = state.GetAdaptiveFilter();
	while (true) {
		if (state.vector_index * STANDARD_VECTOR_SIZE >= state.max_row_group_row) {
			// exceeded the amount of rows to scan
			return;
		}
		idx_t current_row = state.vector_index * STANDARD_VECTOR_SIZE;
		auto max_count = MinValue<idx_t>(STANDARD_VECTOR_SIZE, state.max_row_group_row - current_row);

		//! first check the zonemap if we have to scan this partition
		if (!CheckZonemapSegments(state)) {
			continue;
		}
		// second, scan the version chunk manager to figure out which tuples to load for this transaction
		idx_t count;
		SelectionVector valid_sel(STANDARD_VECTOR_SIZE);
		if (TYPE == TableScanType::TABLE_SCAN_REGULAR) {
			count = state.row_group->GetSelVector(transaction, state.vector_index, valid_sel, max_count);
			if (count == 0) {
				// nothing to scan for this vector, skip the entire vector
				NextVector(state);
				continue;
			}
		} else if (TYPE == TableScanType::TABLE_SCAN_COMMITTED_ROWS_OMIT_PERMANENTLY_DELETED) {
			count = state.row_group->GetCommittedSelVector(transaction.start_time, transaction.transaction_id,
			                                               state.vector_index, valid_sel, max_count);
			if (count == 0) {
				// nothing to scan for this vector, skip the entire vector
				NextVector(state);
				continue;
			}
		} else {
			count = max_count;
		}
		if (count == max_count && !table_filters) {
			// scan all vectors completely: full scan without deletions or table filters
			for (idx_t i = 0; i < column_ids.size(); i++) {
				const auto &column = column_ids[i];
				if (column == COLUMN_IDENTIFIER_ROW_ID) {
					// scan row id
					D_ASSERT(result.data[i].GetType().InternalType() == ROW_TYPE);
					result.data[i].Sequence(this->start + current_row, 1, count);
				} else {
					auto &col_data = GetColumn(column);
					if (TYPE != TableScanType::TABLE_SCAN_REGULAR) {
						col_data.ScanCommitted(state.vector_index, state.column_scans[i], result.data[i],
						                       ALLOW_UPDATES);
					} else {
						col_data.Scan(transaction, state.vector_index, state.column_scans[i], result.data[i]);
					}
				}
			}
		} else {
			// partial scan: we have deletions or table filters
			idx_t approved_tuple_count = count;
			SelectionVector sel;
			if (count != max_count) {
				sel.Initialize(valid_sel);
			} else {
				sel.Initialize(nullptr);
			}
			//! first, we scan the columns with filters, fetch their data and generate a selection vector.
			//! get runtime statistics
			auto start_time = high_resolution_clock::now();
			if (table_filters) {
				D_ASSERT(adaptive_filter);
				D_ASSERT(ALLOW_UPDATES);
				for (idx_t i = 0; i < table_filters->filters.size(); i++) {
					auto tf_idx = adaptive_filter->permutation[i];
					auto col_idx = column_ids[tf_idx];
					auto &col_data = GetColumn(col_idx);
					col_data.Select(transaction, state.vector_index, state.column_scans[tf_idx], result.data[tf_idx],
					                sel, approved_tuple_count, *table_filters->filters[tf_idx]);
				}
				for (auto &table_filter : table_filters->filters) {
					result.data[table_filter.first].Slice(sel, approved_tuple_count);
				}
			}
			if (approved_tuple_count == 0) {
				// all rows were filtered out by the table filters
				// skip this vector in all the scans that were not scanned yet
				D_ASSERT(table_filters);
				result.Reset();
				for (idx_t i = 0; i < column_ids.size(); i++) {
					auto col_idx = column_ids[i];
					if (col_idx == COLUMN_IDENTIFIER_ROW_ID) {
						continue;
					}
					if (table_filters->filters.find(i) == table_filters->filters.end()) {
						auto &col_data = GetColumn(col_idx);
						col_data.Skip(state.column_scans[i]);
					}
				}
				state.vector_index++;
				continue;
			}
			//! Now we use the selection vector to fetch data for the other columns.
			for (idx_t i = 0; i < column_ids.size(); i++) {
				if (!table_filters || table_filters->filters.find(i) == table_filters->filters.end()) {
					auto column = column_ids[i];
					if (column == COLUMN_IDENTIFIER_ROW_ID) {
						D_ASSERT(result.data[i].GetType().InternalType() == PhysicalType::INT64);
						result.data[i].SetVectorType(VectorType::FLAT_VECTOR);
						auto result_data = FlatVector::GetData<int64_t>(result.data[i]);
						for (size_t sel_idx = 0; sel_idx < approved_tuple_count; sel_idx++) {
							result_data[sel_idx] = this->start + current_row + sel.get_index(sel_idx);
						}
					} else {
						auto &col_data = GetColumn(column);
						if (TYPE == TableScanType::TABLE_SCAN_REGULAR) {
							col_data.FilterScan(transaction, state.vector_index, state.column_scans[i], result.data[i],
							                    sel, approved_tuple_count);
						} else {
							col_data.FilterScanCommitted(state.vector_index, state.column_scans[i], result.data[i], sel,
							                             approved_tuple_count, ALLOW_UPDATES);
						}
					}
				}
			}
			auto end_time = high_resolution_clock::now();
			if (adaptive_filter && table_filters->filters.size() > 1) {
				adaptive_filter->AdaptRuntimeStatistics(duration_cast<duration<double>>(end_time - start_time).count());
			}
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

	auto lowest_active_start = transaction_manager.LowestActiveStart();
	auto lowest_active_id = transaction_manager.LowestActiveId();
	TransactionData data(lowest_active_id, lowest_active_start);
	switch (type) {
	case TableScanType::TABLE_SCAN_COMMITTED_ROWS:
		TemplatedScan<TableScanType::TABLE_SCAN_COMMITTED_ROWS>(data, state, result);
		break;
	case TableScanType::TABLE_SCAN_COMMITTED_ROWS_DISALLOW_UPDATES:
		TemplatedScan<TableScanType::TABLE_SCAN_COMMITTED_ROWS_DISALLOW_UPDATES>(data, state, result);
		break;
	case TableScanType::TABLE_SCAN_COMMITTED_ROWS_OMIT_PERMANENTLY_DELETED:
		TemplatedScan<TableScanType::TABLE_SCAN_COMMITTED_ROWS_OMIT_PERMANENTLY_DELETED>(data, state, result);
		break;
	default:
		throw InternalException("Unrecognized table scan type");
	}
}

shared_ptr<RowVersionManager> &RowGroup::GetVersionInfo() {
	if (!HasUnloadedDeletes()) {
		// deletes are loaded - return the version info
		return version_info;
	}
	lock_guard<mutex> lock(row_group_lock);
	// double-check after obtaining the lock whether or not deletes are still not loaded to avoid double load
	if (HasUnloadedDeletes()) {
		// deletes are not loaded - reload
		auto root_delete = deletes_pointers[0];
		version_info = RowVersionManager::Deserialize(root_delete, GetBlockManager().GetMetadataManager(), start);
		deletes_is_loaded = true;
	}
	return version_info;
}

shared_ptr<RowVersionManager> &RowGroup::GetOrCreateVersionInfoPtr() {
	auto vinfo = GetVersionInfo();
	if (!vinfo) {
		lock_guard<mutex> lock(row_group_lock);
		if (!version_info) {
			version_info = make_shared<RowVersionManager>(start);
		}
	}
	return version_info;
}

RowVersionManager &RowGroup::GetOrCreateVersionInfo() {
	return *GetOrCreateVersionInfoPtr();
}

idx_t RowGroup::GetSelVector(TransactionData transaction, idx_t vector_idx, SelectionVector &sel_vector,
                             idx_t max_count) {
	auto &vinfo = GetVersionInfo();
	if (!vinfo) {
		return max_count;
	}
	return vinfo->GetSelVector(transaction, vector_idx, sel_vector, max_count);
}

idx_t RowGroup::GetCommittedSelVector(transaction_t start_time, transaction_t transaction_id, idx_t vector_idx,
                                      SelectionVector &sel_vector, idx_t max_count) {
	auto &vinfo = GetVersionInfo();
	if (!vinfo) {
		return max_count;
	}
	return vinfo->GetCommittedSelVector(start_time, transaction_id, vector_idx, sel_vector, max_count);
}

bool RowGroup::Fetch(TransactionData transaction, idx_t row) {
	D_ASSERT(row < this->count);
	auto &vinfo = GetVersionInfo();
	if (!vinfo) {
		return true;
	}
	return vinfo->Fetch(transaction, row);
}

void RowGroup::FetchRow(TransactionData transaction, ColumnFetchState &state, const vector<column_t> &column_ids,
                        row_t row_id, DataChunk &result, idx_t result_idx) {
	for (idx_t col_idx = 0; col_idx < column_ids.size(); col_idx++) {
		auto column = column_ids[col_idx];
		if (column == COLUMN_IDENTIFIER_ROW_ID) {
			// row id column: fill in the row ids
			D_ASSERT(result.data[col_idx].GetType().InternalType() == PhysicalType::INT64);
			result.data[col_idx].SetVectorType(VectorType::FLAT_VECTOR);
			auto data = FlatVector::GetData<row_t>(result.data[col_idx]);
			data[result_idx] = row_id;
		} else {
			// regular column: fetch data from the base column
			auto &col_data = GetColumn(column);
			col_data.FetchRow(transaction, state, row_id, result.data[col_idx], result_idx);
		}
	}
}

void RowGroup::AppendVersionInfo(TransactionData transaction, idx_t count) {
	idx_t row_group_start = this->count.load();
	idx_t row_group_end = row_group_start + count;
	if (row_group_end > Storage::ROW_GROUP_SIZE) {
		row_group_end = Storage::ROW_GROUP_SIZE;
	}
	// create the version_info if it doesn't exist yet
	auto &vinfo = GetOrCreateVersionInfo();
	vinfo.AppendVersionInfo(transaction, count, row_group_start, row_group_end);
	this->count = row_group_end;
}

void RowGroup::CommitAppend(transaction_t commit_id, idx_t row_group_start, idx_t count) {
	auto &vinfo = GetOrCreateVersionInfo();
	vinfo.CommitAppend(commit_id, row_group_start, count);
}

void RowGroup::RevertAppend(idx_t row_group_start) {
	auto &vinfo = GetOrCreateVersionInfo();
	vinfo.RevertAppend(row_group_start - this->start);
	for (auto &column : columns) {
		column->RevertAppend(row_group_start);
	}
	this->count = MinValue<idx_t>(row_group_start - this->start, this->count);
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
	for (idx_t i = 0; i < GetColumnCount(); i++) {
		auto &col_data = GetColumn(i);
		col_data.Append(state.states[i], chunk.data[i], append_count);
	}
	state.offset_in_row_group += append_count;
}

void RowGroup::Update(TransactionData transaction, DataChunk &update_chunk, row_t *ids, idx_t offset, idx_t count,
                      const vector<PhysicalIndex> &column_ids) {
#ifdef DEBUG
	for (size_t i = offset; i < offset + count; i++) {
		D_ASSERT(ids[i] >= row_t(this->start) && ids[i] < row_t(this->start + this->count));
	}
#endif
	for (idx_t i = 0; i < column_ids.size(); i++) {
		auto column = column_ids[i];
		D_ASSERT(column.index != COLUMN_IDENTIFIER_ROW_ID);
		auto &col_data = GetColumn(column.index);
		D_ASSERT(col_data.type.id() == update_chunk.data[i].GetType().id());
		if (offset > 0) {
			Vector sliced_vector(update_chunk.data[i], offset, offset + count);
			sliced_vector.Flatten(count);
			col_data.Update(transaction, column.index, sliced_vector, ids + offset, count);
		} else {
			col_data.Update(transaction, column.index, update_chunk.data[i], ids, count);
		}
		MergeStatistics(column.index, *col_data.GetUpdateStatistics());
	}
}

void RowGroup::UpdateColumn(TransactionData transaction, DataChunk &updates, Vector &row_ids,
                            const vector<column_t> &column_path) {
	D_ASSERT(updates.ColumnCount() == 1);
	auto ids = FlatVector::GetData<row_t>(row_ids);

	auto primary_column_idx = column_path[0];
	D_ASSERT(primary_column_idx != COLUMN_IDENTIFIER_ROW_ID);
	D_ASSERT(primary_column_idx < columns.size());
	auto &col_data = GetColumn(primary_column_idx);
	col_data.UpdateColumn(transaction, column_path, updates.data[0], ids, updates.size(), 1);
	MergeStatistics(primary_column_idx, *col_data.GetUpdateStatistics());
}

unique_ptr<BaseStatistics> RowGroup::GetStatistics(idx_t column_idx) {
	auto &col_data = GetColumn(column_idx);
	lock_guard<mutex> slock(stats_lock);
	return col_data.GetStatistics();
}

void RowGroup::MergeStatistics(idx_t column_idx, const BaseStatistics &other) {
	auto &col_data = GetColumn(column_idx);
	lock_guard<mutex> slock(stats_lock);
	col_data.MergeStatistics(other);
}

void RowGroup::MergeIntoStatistics(idx_t column_idx, BaseStatistics &other) {
	auto &col_data = GetColumn(column_idx);
	lock_guard<mutex> slock(stats_lock);
	col_data.MergeIntoStatistics(other);
}

RowGroupWriteData RowGroup::WriteToDisk(PartialBlockManager &manager,
                                        const vector<CompressionType> &compression_types) {
	RowGroupWriteData result;
	result.states.reserve(columns.size());
	result.statistics.reserve(columns.size());

	// Checkpoint the individual columns of the row group
	// Here we're iterating over columns. Each column can have multiple segments.
	// (Some columns will be wider than others, and require different numbers
	// of blocks to encode.) Segments cannot span blocks.
	//
	// Some of these columns are composite (list, struct). The data is written
	// first sequentially, and the pointers are written later, so that the
	// pointers all end up densely packed, and thus more cache-friendly.
	for (idx_t column_idx = 0; column_idx < GetColumnCount(); column_idx++) {
		auto &column = GetColumn(column_idx);
		ColumnCheckpointInfo checkpoint_info {compression_types[column_idx]};
		auto checkpoint_state = column.Checkpoint(*this, manager, checkpoint_info);
		D_ASSERT(checkpoint_state);

		auto stats = checkpoint_state->GetStatistics();
		D_ASSERT(stats);

		result.statistics.push_back(stats->Copy());
		result.states.push_back(std::move(checkpoint_state));
	}
	D_ASSERT(result.states.size() == result.statistics.size());
	return result;
}

bool RowGroup::AllDeleted() {
	if (HasUnloadedDeletes()) {
		// deletes aren't loaded yet - we know not everything is deleted
		return false;
	}
	auto &vinfo = GetVersionInfo();
	if (!vinfo) {
		return false;
	}
	return vinfo->GetCommittedDeletedCount(count) == count;
}

bool RowGroup::HasUnloadedDeletes() const {
	if (deletes_pointers.empty()) {
		// no stored deletes at all
		return false;
	}
	// return whether or not the deletes have been loaded
	return !deletes_is_loaded;
}

RowGroupPointer RowGroup::Checkpoint(RowGroupWriter &writer, TableStatistics &global_stats) {
	RowGroupPointer row_group_pointer;

	vector<CompressionType> compression_types;
	compression_types.reserve(columns.size());
	for (idx_t column_idx = 0; column_idx < GetColumnCount(); column_idx++) {
		compression_types.push_back(writer.GetColumnCompressionType(column_idx));
	}
	auto result = WriteToDisk(writer.GetPartialBlockManager(), compression_types);
	for (idx_t column_idx = 0; column_idx < GetColumnCount(); column_idx++) {
		global_stats.GetStats(column_idx).Statistics().Merge(result.statistics[column_idx]);
	}

	// construct the row group pointer and write the column meta data to disk
	D_ASSERT(result.states.size() == columns.size());
	row_group_pointer.row_start = start;
	row_group_pointer.tuple_count = count;
	for (auto &state : result.states) {
		// get the current position of the table data writer
		auto &data_writer = writer.GetPayloadWriter();
		auto pointer = data_writer.GetMetaBlockPointer();

		// store the stats and the data pointers in the row group pointers
		row_group_pointer.data_pointers.push_back(pointer);

		// Write pointers to the column segments.
		//
		// Just as above, the state can refer to many other states, so this
		// can cascade recursively into more pointer writes.
		BinarySerializer serializer(data_writer);
		serializer.Begin();
		state->WriteDataPointers(writer, serializer);
		serializer.End();
	}
	row_group_pointer.deletes_pointers = CheckpointDeletes(writer.GetPayloadWriter().GetManager());
	Verify();
	return row_group_pointer;
}

vector<MetaBlockPointer> RowGroup::CheckpointDeletes(MetadataManager &manager) {
	if (HasUnloadedDeletes()) {
		// deletes were not loaded so they cannot be changed
		// re-use them as-is
		manager.ClearModifiedBlocks(deletes_pointers);
		return deletes_pointers;
	}
	if (!version_info) {
		// no version information: write nothing
		return vector<MetaBlockPointer>();
	}
	return version_info->Checkpoint(manager);
}

void RowGroup::Serialize(RowGroupPointer &pointer, Serializer &serializer) {
	serializer.WriteProperty(100, "row_start", pointer.row_start);
	serializer.WriteProperty(101, "tuple_count", pointer.tuple_count);
	serializer.WriteProperty(102, "data_pointers", pointer.data_pointers);
	serializer.WriteProperty(103, "delete_pointers", pointer.deletes_pointers);
}

RowGroupPointer RowGroup::Deserialize(Deserializer &deserializer) {
	RowGroupPointer result;
	result.row_start = deserializer.ReadProperty<uint64_t>(100, "row_start");
	result.tuple_count = deserializer.ReadProperty<uint64_t>(101, "tuple_count");
	result.data_pointers = deserializer.ReadProperty<vector<MetaBlockPointer>>(102, "data_pointers");
	result.deletes_pointers = deserializer.ReadProperty<vector<MetaBlockPointer>>(103, "delete_pointers");
	return result;
}

//===--------------------------------------------------------------------===//
// GetColumnSegmentInfo
//===--------------------------------------------------------------------===//
void RowGroup::GetColumnSegmentInfo(idx_t row_group_index, vector<ColumnSegmentInfo> &result) {
	for (idx_t col_idx = 0; col_idx < GetColumnCount(); col_idx++) {
		auto &col_data = GetColumn(col_idx);
		col_data.GetColumnSegmentInfo(row_group_index, {col_idx}, result);
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

idx_t RowGroup::Delete(TransactionData transaction, DataTable &table, row_t *ids, idx_t count) {
	VersionDeleteState del_state(*this, transaction, table, this->start);

	// obtain a write lock
	for (idx_t i = 0; i < count; i++) {
		D_ASSERT(ids[i] >= 0);
		D_ASSERT(idx_t(ids[i]) >= this->start && idx_t(ids[i]) < this->start + this->count);
		del_state.Delete(ids[i] - this->start);
	}
	del_state.Flush();
	return del_state.delete_count;
}

void RowGroup::Verify() {
#ifdef DEBUG
	for (auto &column : GetColumns()) {
		column->Verify(*this);
	}
#endif
}

idx_t RowGroup::DeleteRows(idx_t vector_idx, transaction_t transaction_id, row_t rows[], idx_t count) {
	return GetOrCreateVersionInfo().DeleteRows(vector_idx, transaction_id, rows, count);
}

void VersionDeleteState::Delete(row_t row_id) {
	D_ASSERT(row_id >= 0);
	idx_t vector_idx = row_id / STANDARD_VECTOR_SIZE;
	idx_t idx_in_vector = row_id - vector_idx * STANDARD_VECTOR_SIZE;
	if (current_chunk != vector_idx) {
		Flush();

		current_chunk = vector_idx;
		chunk_row = vector_idx * STANDARD_VECTOR_SIZE;
	}
	rows[count++] = idx_in_vector;
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
