#include "duckdb/storage/buffer/system_buffer_handle.hpp"
#include "duckdb/storage/buffer_manager.hpp"

namespace duckdb {

SystemBufferHandle::SystemBufferHandle() : handle(nullptr), node(nullptr) {
}

SystemBufferHandle::SystemBufferHandle(shared_ptr<BlockHandle> handle, FileBuffer *node)
    : handle(move(handle)), node(node) {
}

SystemBufferHandle::SystemBufferHandle(SystemBufferHandle &&other) noexcept {
	std::swap(node, other.node);
	std::swap(handle, other.handle);
}

SystemBufferHandle &SystemBufferHandle::operator=(SystemBufferHandle &&other) noexcept {
	std::swap(node, other.node);
	std::swap(handle, other.handle);
	return *this;
}

SystemBufferHandle::~SystemBufferHandle() {
	Destroy();
}

bool SystemBufferHandle::IsValid() const {
	return node != nullptr;
}

data_ptr_t SystemBufferHandle::Ptr() const {
	D_ASSERT(IsValid());
	return node->buffer;
}

data_ptr_t SystemBufferHandle::Ptr() {
	D_ASSERT(IsValid());
	return node->buffer;
}

void SystemBufferHandle::Destroy() {
	if (!handle || !IsValid()) {
		return;
	}
	handle->block_manager.buffer_manager.Unpin(handle);
	handle.reset();
	node = nullptr;
}

FileBuffer &SystemBufferHandle::GetFileBuffer() {
	D_ASSERT(node);
	return *node;
}

const shared_ptr<BlockHandle> &SystemBufferHandle::GetBlockHandle() const {
	return handle;
}

} // namespace duckdb
