#include "duckdb/storage/block.hpp"

#include "duckdb/common/assert.hpp"

namespace duckdb {

Block::Block(Allocator &allocator, const block_id_t id, const idx_t block_size)
    : FileBuffer(allocator, FileBufferType::BLOCK, block_size), id(id) {
}

Block::Block(Allocator &allocator, block_id_t id, uint32_t internal_size)
    : FileBuffer(allocator, FileBufferType::BLOCK, internal_size), id(id) {
	D_ASSERT((AllocSize() & (Storage::SECTOR_SIZE - 1)) == 0);
}

Block::Block(FileBuffer &source, block_id_t id) : FileBuffer(source, FileBufferType::BLOCK), id(id) {
	D_ASSERT((AllocSize() & (Storage::SECTOR_SIZE - 1)) == 0);
}

} // namespace duckdb
