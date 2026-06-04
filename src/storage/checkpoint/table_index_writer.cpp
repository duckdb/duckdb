#include "duckdb/storage/checkpoint/table_index_writer.hpp"
#include "duckdb/storage/partial_block_manager.hpp"

namespace duckdb {

TableIndexWriter::TableIndexWriter(PartialBlockManager &partial_block_manager, IndexSerializationInfo &info) : partial_block_manager(partial_block_manager), info(info) {
}

TableIndexWriter::~TableIndexWriter() {
}

void TableIndexWriter::AddUnboundIndex(const IndexStorageInfo &storage_info) {
	result.ordered_infos.push_back(storage_info);
}

void TableIndexWriter::AddBoundIndex(IndexStorageInfo storage_info, unique_ptr<BoundIndex> index) {
	result.bound_infos.push_back(std::move(storage_info));
	result.ordered_infos.push_back(result.bound_infos.back());

	indexes.push_back(std::move(index));
}

SingleFileIndexWriter::SingleFileIndexWriter(PartialBlockManager &partial_block_manager, IndexSerializationInfo &info): TableIndexWriter(partial_block_manager, info) {
}

} // namespace duckdb