#include "duckdb/storage/buffer/managed_buffer.hpp"

#include "duckdb/common/allocator.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/config.hpp"

namespace duckdb {

ManagedBuffer::ManagedBuffer(DatabaseInstance &db, idx_t size, bool can_destroy, block_id_t id)
    : FileBuffer(Allocator::Get(db), FileBufferType::MANAGED_BUFFER, DBConfig::GetConfig(db).use_direct_io, size),
      db(db), can_destroy(can_destroy), id(id) {
	D_ASSERT(id >= MAXIMUM_BLOCK);
	D_ASSERT(size >= Storage::BLOCK_ALLOC_SIZE);
}

} // namespace duckdb
