#include "duckdb/storage/buffer/buffer_handle.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/buffer/block_handle.hpp"

namespace duckdb {

BufferHandle::BufferHandle() : handle(nullptr), node(nullptr), buffer_ptr(nullptr) {
}

BufferHandle::BufferHandle(shared_ptr<BlockHandle> handle_p, FileBuffer *node_p)
    : handle(std::move(handle_p)), node(node_p), buffer_ptr(node->buffer) {
}

BufferHandle::BufferHandle(BufferHandle &&other) noexcept {
	std::swap(node, other.node);
	std::swap(handle, other.handle);
	std::swap(buffer_ptr, other.buffer_ptr);
}

BufferHandle &BufferHandle::operator=(BufferHandle &&other) noexcept {
	std::swap(node, other.node);
	std::swap(handle, other.handle);
	std::swap(buffer_ptr, other.buffer_ptr);
	return *this;
}

BufferHandle::~BufferHandle() {
	Destroy();
}

bool BufferHandle::IsValid() const {
	return node != nullptr;
}

void BufferHandle::Destroy() {
	if (!handle || !IsValid()) {
		return;
	}
	handle->block_manager.buffer_manager.Unpin(handle);
	handle.reset();
	node = nullptr;
	buffer_ptr = nullptr;
}

FileBuffer &BufferHandle::GetFileBuffer() {
	D_ASSERT(node);
	return *node;
}

} // namespace duckdb
