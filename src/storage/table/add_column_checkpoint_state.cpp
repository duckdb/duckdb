#include "duckdb/storage/table/add_column_checkpoint_state.hpp"
#include "duckdb/storage/table/column_checkpoint_state.hpp"
#include "duckdb/storage/table/column_data.hpp"
#include "duckdb/storage/table/row_group_collection.hpp"
#include "duckdb/storage/table/row_group_segment_tree.hpp"

namespace duckdb {

AddColumnCheckpointState::AddColumnCheckpointState(ClientContext &context, DataTable &table, AddColumnInfo &info)
    : context(context), table(table), info(info) {
}

AddColumnCheckpointState::~AddColumnCheckpointState() {
}

bool AddColumnCheckpointState::PrepareWrite() {
	// check if we should pre-emptively write the table to disk
	if (table.IsTemporary() || StorageManager::Get(table.GetAttached()).InMemory()) {
		return false;
	}
	// allocate the partial block-manager if none is allocated yet
	if (!partial_manager) {
		auto &block_manager = table.GetTableIOManager().GetBlockManagerForRowData();
		partial_manager =
		    make_uniq<PartialBlockManager>(QueryContext(context), block_manager, PartialBlockType::APPEND_TO_TABLE);
	}
	// initialize compression_types vector if it is empty
	if (compression_types.empty()) {
		for (auto &column : table.Columns()) {
			compression_types.push_back(column.CompressionType());
		}
		compression_types.push_back(info.new_column.CompressionType());
	}

	return true;
}

void AddColumnCheckpointState::FlushToDisk(RowGroupCollection &data) {
	if (!PrepareWrite()) {
		return;
	}

	info.replay_stable_result = true;
	info.stable_result = make_shared_ptr<PersistentCollectionData>();

	// Flush all row groups to disk
	RowGroupWriteInfo write_info(*partial_manager, compression_types);
	for (int64_t i = 0;; ++i) {
		auto row_group_ptr = data.GetRowGroup(i);
		if (row_group_ptr == nullptr)
			break;
		row_group_ptr->WriteLastColumnToDisk(write_info);
	}

	partial_manager->FlushPartialBlocks();
	partial_manager.reset();

	// Prepare `stable_result`, which will be written to WAL later
	for (int64_t i = 0;; ++i) {
		auto row_group_ptr = data.GetRowGroup(i);
		if (row_group_ptr == nullptr)
			break;
		auto persistent_data = row_group_ptr->SerializeRowGroupInfo(true);
		persistent_data.types.push_back(data.GetTypes().back());
		info.stable_result->row_group_data.push_back(std::move(persistent_data));
	}
}

shared_ptr<PersistentCollectionData> AddColumnCheckpointState::GetStableResult() {
	// Using `std::move` in `ColumnData::InitializeColumn` has invalidated `stable_result`. We are replaying the WAL, so
	// we can simply set `replay_stable_result` to false to avoid errors during subsequent Serialize/Deserialize.
	info.replay_stable_result = false;
	return info.stable_result;
}

} // namespace duckdb
