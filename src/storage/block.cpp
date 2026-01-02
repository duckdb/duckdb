#include "duckdb/storage/block.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/assert.hpp"

namespace duckdb {

Block::Block(BlockAllocator &allocator, const block_id_t id, const idx_t block_size, const idx_t block_header_size)
    : FileBuffer(allocator, FileBufferType::BLOCK, block_size, block_header_size), id(id) {
}

Block::Block(BlockAllocator &allocator, block_id_t id, uint32_t internal_size, idx_t block_header_size)
    : FileBuffer(allocator, FileBufferType::BLOCK, internal_size, block_header_size), id(id) {
	D_ASSERT((AllocSize() & (Storage::SECTOR_SIZE - 1)) == 0);
}

Block::Block(BlockAllocator &allocator, block_id_t id, BlockManager &block_manager)
    : FileBuffer(allocator, FileBufferType::BLOCK, block_manager), id(id) {
	D_ASSERT((AllocSize() & (Storage::SECTOR_SIZE - 1)) == 0);
}

Block::Block(FileBuffer &source, block_id_t id, idx_t block_header_size)
    : FileBuffer(source, FileBufferType::BLOCK, block_header_size), id(id) {
	D_ASSERT((AllocSize() & (Storage::SECTOR_SIZE - 1)) == 0);
}

} // namespace duckdb
