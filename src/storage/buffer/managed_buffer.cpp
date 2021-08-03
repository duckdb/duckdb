#include "duckdb/storage/buffer/managed_buffer.hpp"

#include "duckdb/common/allocator.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/config.hpp"

namespace duckdb {

ManagedBuffer::ManagedBuffer(DatabaseInstance &db, idx_t size, bool can_destroy, block_id_t id)
    : FileBuffer(Allocator::Get(db), FileBufferType::MANAGED_BUFFER, size), db(db), can_destroy(can_destroy), id(id) {
	D_ASSERT(id >= MAXIMUM_BLOCK);
	D_ASSERT(size >= Storage::BLOCK_SIZE);
}

void ManagedBuffer::Read(FileHandle &handle, uint64_t location) {
	handle.Read(internal_buffer, internal_size, location);
}

void ManagedBuffer::Write(FileHandle &handle, uint64_t location) {
	handle.Write(internal_buffer, internal_size, location);
}

} // namespace duckdb
