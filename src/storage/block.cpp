#include "duckdb/storage/block.hpp"

namespace duckdb {

Block::Block(DatabaseInstance &db, block_id_t id)
    : FileBuffer(Allocator::Get(db), FileBufferType::BLOCK, DBConfig::GetConfig(db).use_direct_io,
                 Storage::BLOCK_ALLOC_SIZE),
      id(id) {
}

} // namespace duckdb
