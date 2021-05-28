#include "duckdb/storage/buffer/managed_buffer.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/allocator.hpp"

namespace duckdb {

ManagedBuffer::ManagedBuffer(DatabaseInstance &db, idx_t size, bool can_destroy, block_id_t id)
    : FileBuffer(Allocator::Get(db), FileBufferType::MANAGED_BUFFER, size), db(db), can_destroy(can_destroy), id(id) {
	D_ASSERT(id >= MAXIMUM_BLOCK);
	D_ASSERT(size >= Storage::BLOCK_ALLOC_SIZE);
}

} // namespace duckdb
