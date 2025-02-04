#include "duckdb/execution/index/unbound_index.hpp"
#include "duckdb/parser/parsed_data/create_index_info.hpp"
#include "duckdb/storage/table_io_manager.hpp"
#include "duckdb/storage/block_manager.hpp"
#include "duckdb/storage/index_storage_info.hpp"

namespace duckdb {

//-------------------------------------------------------------------------------
// Unbound index
//-------------------------------------------------------------------------------

UnboundIndex::UnboundIndex(unique_ptr<CreateInfo> create_info, IndexStorageInfo storage_info_p,
                           TableIOManager &table_io_manager, AttachedDatabase &db)
    : Index(create_info->Cast<CreateIndexInfo>().column_ids, table_io_manager, db), create_info(std::move(create_info)),
      storage_info(std::move(storage_info_p)) {

	// Memory safety check.
	for (idx_t info_idx = 0; info_idx < storage_info.allocator_infos.size(); info_idx++) {
		auto &info = storage_info.allocator_infos[info_idx];
		for (idx_t buffer_idx = 0; buffer_idx < info.buffer_ids.size(); buffer_idx++) {
			if (info.buffer_ids[buffer_idx] > idx_t(MAX_ROW_ID)) {
				throw InternalException("Found invalid buffer ID in UnboundIndex constructor");
			}
		}
	}
}

void UnboundIndex::CommitDrop() {
	auto &block_manager = table_io_manager.GetIndexBlockManager();
	for (auto &info : storage_info.allocator_infos) {
		for (auto &block : info.block_pointers) {
			if (block.IsValid()) {
				block_manager.MarkBlockAsModified(block.block_id);
			}
		}
	}
}

} // namespace duckdb
