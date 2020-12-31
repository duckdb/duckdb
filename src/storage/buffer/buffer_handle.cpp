#include "duckdb/storage/buffer/buffer_handle.hpp"
#include "duckdb/storage/buffer_manager.hpp"

namespace duckdb {

BufferHandle::BufferHandle(BufferManager &manager, shared_ptr<BlockHandle> handle, FileBuffer *node)
    : manager(manager), handle(move(handle)), node(node) {
}

BufferHandle::~BufferHandle() {
	manager.Unpin(handle);
}

data_ptr_t BufferHandle::Ptr() {
	return node->buffer;
}

} // namespace duckdb
