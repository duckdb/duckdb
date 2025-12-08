#include "duckdb/storage/optimistic_data_writer.hpp"
#include "duckdb/storage/table/column_segment.hpp"
#include "duckdb/storage/partial_block_manager.hpp"
#include "duckdb/storage/table/column_checkpoint_state.hpp"
#include "duckdb/main/settings.hpp"

namespace duckdb {

OptimisticWriteCollection::~OptimisticWriteCollection() {
}

OptimisticDataWriter::OptimisticDataWriter(ClientContext &context, DataTable &table) : context(context), table(table) {
}

OptimisticDataWriter::OptimisticDataWriter(DataTable &table, OptimisticDataWriter &parent)
    : context(parent.GetClientContext()), table(table) {
	if (parent.partial_manager) {
		parent.partial_manager->ClearBlocks();
	}
}

OptimisticDataWriter::~OptimisticDataWriter() {
}

bool OptimisticDataWriter::PrepareWrite() {
	// check if we should pre-emptively write the table to disk
	if (table.IsTemporary() || StorageManager::Get(table.GetAttached()).InMemory()) {
		return false;
	}
	// we should! write the second-to-last row group to disk
	// allocate the partial block-manager if none is allocated yet
	if (!partial_manager) {
		auto &block_manager = table.GetTableIOManager().GetBlockManagerForRowData();
		partial_manager = make_uniq<PartialBlockManager>(context, block_manager, PartialBlockType::APPEND_TO_TABLE);
	}
	return true;
}

unique_ptr<OptimisticWriteCollection> OptimisticDataWriter::CreateCollection(DataTable &storage,
                                                                             const vector<LogicalType> &insert_types,
                                                                             OptimisticWritePartialManagers type) {
	auto table_info = storage.GetDataTableInfo();
	auto &io_manager = TableIOManager::Get(storage);

	// Create the local row group collection.
	auto max_row_id = NumericCast<idx_t>(MAX_ROW_ID);
	auto row_groups = make_shared_ptr<RowGroupCollection>(std::move(table_info), io_manager, insert_types, max_row_id);

	auto result = make_uniq<OptimisticWriteCollection>();
	result->collection = std::move(row_groups);
	if (type == OptimisticWritePartialManagers::PER_COLUMN) {
		for (idx_t i = 0; i < insert_types.size(); i++) {
			auto &block_manager = table.GetTableIOManager().GetBlockManagerForRowData();
			result->partial_block_managers.push_back(make_uniq<PartialBlockManager>(
			    QueryContext(context), block_manager, PartialBlockType::APPEND_TO_TABLE));
		}
	}
	return result;
}

void OptimisticDataWriter::WriteNewRowGroup(OptimisticWriteCollection &row_groups) {
	// we finished writing a complete row group
	if (!PrepareWrite()) {
		return;
	}

	row_groups.complete_row_groups++;
	auto unflushed_row_groups = row_groups.complete_row_groups - row_groups.last_flushed;
	if (unflushed_row_groups >= DBConfig::GetSetting<WriteBufferRowGroupCountSetting>(context)) {
		// we have crossed our flush threshold - flush any unwritten row groups to disk
		vector<const_reference<RowGroup>> to_flush;
		vector<int64_t> segment_indexes;
		for (idx_t i = row_groups.last_flushed; i < row_groups.complete_row_groups; i++) {
			auto segment_index = NumericCast<int64_t>(i);
			to_flush.push_back(*row_groups.collection->GetRowGroup(segment_index));
			segment_indexes.push_back(segment_index);
		}
		FlushToDisk(row_groups, to_flush, segment_indexes);
		row_groups.last_flushed = row_groups.complete_row_groups;
	}
}

void OptimisticDataWriter::WriteLastRowGroup(OptimisticWriteCollection &row_groups) {
	// we finished writing a complete row group
	if (!PrepareWrite()) {
		return;
	}
	// flush the last batch of row groups
	vector<const_reference<RowGroup>> to_flush;
	vector<int64_t> segment_indexes;
	for (idx_t i = row_groups.last_flushed; i < row_groups.complete_row_groups; i++) {
		auto segment_index = NumericCast<int64_t>(i);
		to_flush.push_back(*row_groups.collection->GetRowGroup(segment_index));
		segment_indexes.push_back(segment_index);
	}
	// add the last (incomplete) row group
	to_flush.push_back(*row_groups.collection->GetRowGroup(-1));
	segment_indexes.push_back(-1);

	FlushToDisk(row_groups, to_flush, segment_indexes);

	for (auto &partial_manager : row_groups.partial_block_managers) {
		Merge(partial_manager);
	}
	row_groups.partial_block_managers.clear();
}

void OptimisticDataWriter::FlushToDisk(OptimisticWriteCollection &collection,
                                       const vector<const_reference<RowGroup>> &row_groups,
                                       const vector<int64_t> &segment_indexes) {
	//! The set of column compression types (if any)
	vector<CompressionType> compression_types;
	D_ASSERT(compression_types.empty());
	for (auto &column : table.Columns()) {
		compression_types.push_back(column.CompressionType());
	}
	RowGroupWriteInfo info(*partial_manager, compression_types, collection.partial_block_managers);
	auto result = RowGroup::WriteToDisk(info, row_groups);
	// move new (checkpointed) row groups to the row group collection
	for (idx_t i = 0; i < row_groups.size(); i++) {
		collection.collection->SetRowGroup(segment_indexes[i], std::move(result[i].result_row_group));
	}
}

void OptimisticDataWriter::Merge(unique_ptr<PartialBlockManager> &other_manager) {
	if (!other_manager) {
		return;
	}
	if (!partial_manager) {
		partial_manager = std::move(other_manager);
		return;
	}
	partial_manager->Merge(*other_manager);
	other_manager.reset();
}

void OptimisticDataWriter::Merge(OptimisticDataWriter &other) {
	Merge(other.partial_manager);
}

void OptimisticDataWriter::FinalFlush() {
	if (partial_manager) {
		partial_manager->FlushPartialBlocks();
		partial_manager.reset();
	}
}

void OptimisticDataWriter::Rollback() {
	if (partial_manager) {
		partial_manager->Rollback();
		partial_manager.reset();
	}
}

} // namespace duckdb
