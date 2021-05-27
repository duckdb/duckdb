#include "duckdb/storage/block.hpp"

namespace duckdb {

Block::Block(Allocator &allocator, block_id_t id) :
	FileBuffer(allocator, FileBufferType::BLOCK, Storage::BLOCK_ALLOC_SIZE), id(id) {
}

} // namespace duckdb
