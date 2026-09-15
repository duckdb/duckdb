#include "duckdb/storage/checkpoint/table_index_writer.hpp"
#include "duckdb/common/shared_ptr_ipp.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/execution/index/bound_index.hpp"
#include "duckdb/storage/index_storage_info.hpp"
#include "duckdb/storage/table/table_index_list.hpp"
#include "duckdb/storage/partial_block_manager.hpp"
#include "duckdb/storage/checkpoint_manager.hpp"

namespace duckdb {

TableIndexWriter::TableIndexWriter(PartialBlockManager &partial_block_manager, StorageVersion version)
    : partial_block_manager(partial_block_manager), storage_version(version) {
}

TableIndexWriter::~TableIndexWriter() {
}

StorageVersion TableIndexWriter::GetStorageVersion() const {
	return storage_version;
}

SingleFileTableIndexWriter::SingleFileTableIndexWriter(SingleFileCheckpointWriter &checkpoint_manager,
                                                       const StorageVersion version, const bool debug_verify_blocks)
    : TableIndexWriter(checkpoint_manager.index_partial_block_manager, version), checkpoint_manager(checkpoint_manager),
      debug_verify_blocks(debug_verify_blocks) {
}

PartialBlockManager SingleFileTableIndexWriter::CreateIsolatedPartialBlockManager() {
	return checkpoint_manager.CreateIsolatedIndexPartialBlockManager();
}

void SingleFileTableIndexWriter::Flush() {
	partial_block_manager.FlushPartialBlocks();
}

void SingleFileTableIndexWriter::VerifyBlockUsage(const vector<shared_ptr<const IndexStorageInfo>> &infos) {
	for (const auto &storage_info : infos) {
		for (auto &allocator : storage_info->allocator_infos) {
			for (auto &block : allocator.block_pointers) {
				checkpoint_manager.verify_block_usage_count[block.block_id]++;
			}
		}
	}
}

} // namespace duckdb
