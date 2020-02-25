#include "duckdb/storage/buffer/managed_buffer.hpp"
#include "duckdb/common/exception.hpp"

using namespace duckdb;
using namespace std;

ManagedBuffer::ManagedBuffer(BufferManager &manager, idx_t size, bool can_destroy, block_id_t id)
    : FileBuffer(FileBufferType::MANAGED_BUFFER, size), manager(manager), can_destroy(can_destroy), id(id) {
	assert(id >= MAXIMUM_BLOCK);
	assert(size >= Storage::BLOCK_ALLOC_SIZE);
}
