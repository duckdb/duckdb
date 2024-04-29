#include "duckdb/execution/index/unbound_index.hpp"
#include "duckdb/parser/parsed_data/create_index_info.hpp"
#include "duckdb/storage/table_io_manager.hpp"
#include "duckdb/storage/block_manager.hpp"
#include "duckdb/storage/index_storage_info.hpp"

namespace duckdb {

//-------------------------------------------------------------------------------
// Unbound index
//-------------------------------------------------------------------------------

UnboundIndex::UnboundIndex(const CreateIndexInfo &info, IndexStorageInfo storage_info_p,
                           TableIOManager &table_io_manager, AttachedDatabase &db)
    : Index(info.index_name, info.index_type, info.constraint_type, info.column_ids, table_io_manager, db),
      create_info(info), storage_info(std::move(storage_info_p)) {

	// Annoyingly, parsed expressions are not copied in the CreateIndexInfo copy constructor
	for (auto &expr : info.parsed_expressions) {
		create_info.parsed_expressions.push_back(expr->Copy());
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
