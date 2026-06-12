#include "duckdb/storage/checkpoint/table_index_writer.hpp"
#include "duckdb/storage/partial_block_manager.hpp"

namespace duckdb {

TableIndexWriter::TableIndexWriter(PartialBlockManager &partial_block_manager, StorageVersion version)
    : partial_block_manager(partial_block_manager), storage_version(version) {
}

TableIndexWriter::~TableIndexWriter() {
}

void TableIndexWriter::ReserveBoundIndexes(idx_t count) {
	result.bound_infos.reserve(count);
	indexes.reserve(count);
}

void TableIndexWriter::AddUnboundIndex(const IndexStorageInfo &storage_info) {
	result.ordered_infos.push_back(storage_info);
}

void TableIndexWriter::AddBoundIndex(IndexStorageInfo storage_info, unique_ptr<BoundIndex> index) {
	result.bound_infos.push_back(std::move(storage_info));
	result.ordered_infos.push_back(result.bound_infos.back());

	indexes.push_back(std::move(index));
}

SingleFileIndexWriter::SingleFileIndexWriter(PartialBlockManager &partial_block_manager, StorageVersion version)
    : TableIndexWriter(partial_block_manager, version) {
}

void SingleFileIndexWriter::FlushPartialBlocks() {
	partial_block_manager.FlushPartialBlocks();
}

IndexSerializationFormat SingleFileIndexWriter::GetTargetFormat() const {
	const auto v1_0_0_storage = StorageManager::IsPriorToVersion(StorageVersion::V1_2_0, storage_version);
	if (v1_0_0_storage) {
		return IndexSerializationFormat::V1_0_0;
	}

	return IndexSerializationFormat::CURRENT;
}

} // namespace duckdb
