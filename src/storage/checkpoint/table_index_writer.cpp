#include "duckdb/storage/checkpoint/table_index_writer.hpp"
#include "duckdb/storage/table/table_index_list.hpp"
#include "duckdb/storage/partial_block_manager.hpp"

namespace duckdb {

TableIndexWriter::TableIndexWriter(PartialBlockManager &partial_block_manager, StorageVersion version)
    : partial_block_manager(partial_block_manager), storage_version(version) {
}

TableIndexWriter::~TableIndexWriter() {
}

void TableIndexWriter::AddUnboundIndex(shared_ptr<const IndexStorageInfo> info) {
	result.push_back({std::move(info), nullptr});
}

void TableIndexWriter::AddBoundIndex(IndexStorageInfo info, unique_ptr<BoundIndex> index) {
	result.push_back({make_shared_ptr<const IndexStorageInfo>(std::move(info)), std::move(index)});
}

unique_ptr<BoundIndex> TableIndexWriter::TakeShadowIndex(idx_t index) {
	D_ASSERT(index < result.size());
	return std::move(result[index].shadow_index);
}

StorageVersion TableIndexWriter::GetStorageVersion() const {
	return storage_version;
}

SingleFileIndexWriter::SingleFileIndexWriter(SingleFileCheckpointWriter &checkpoint_manager,
                                             PartialBlockManager &partial_block_manager, const StorageVersion version,
                                             const bool debug_verify_blocks)
    : TableIndexWriter(partial_block_manager, version), checkpoint_manager(checkpoint_manager),
      debug_verify_blocks(debug_verify_blocks) {
}

void SingleFileIndexWriter::Flush() {
	partial_block_manager.FlushPartialBlocks();
}

void SingleFileIndexWriter::Serialize(Serializer &serializer) {
	if (debug_verify_blocks) {
		VerifyBlockUsage();
	}

	TableIndexList::Serialize(result, serializer);
}

void SingleFileIndexWriter::VerifyBlockUsage() {
	for (const auto &[storage_info, _] : result) {
		for (auto &allocator : storage_info->allocator_infos) {
			for (auto &block : allocator.block_pointers) {
				checkpoint_manager.verify_block_usage_count[block.block_id]++;
			}
		}
	}
}

} // namespace duckdb
