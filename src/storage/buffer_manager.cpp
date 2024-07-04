#include "duckdb/storage/buffer_manager.hpp"

#include "duckdb/common/allocator.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_buffer.hpp"
#include "duckdb/storage/buffer/buffer_pool.hpp"
#include "duckdb/storage/standard_buffer_manager.hpp"

namespace duckdb {

shared_ptr<BlockHandle> BufferManager::RegisterTransientMemory(const idx_t size, const idx_t block_size) {
	throw NotImplementedException("This type of BufferManager can not create 'transient-memory' blocks");
}

shared_ptr<BlockHandle> BufferManager::RegisterSmallMemory(const idx_t size) {
	throw NotImplementedException("This type of BufferManager can not create 'small-memory' blocks");
}

Allocator &BufferManager::GetBufferAllocator() {
	throw NotImplementedException("This type of BufferManager does not have an Allocator");
}

void BufferManager::ReserveMemory(idx_t size) {
	throw NotImplementedException("This type of BufferManager can not reserve memory");
}
void BufferManager::FreeReservedMemory(idx_t size) {
	throw NotImplementedException("This type of BufferManager can not free reserved memory");
}

void BufferManager::SetMemoryLimit(idx_t limit) {
	throw NotImplementedException("This type of BufferManager can not set a memory limit");
}

void BufferManager::SetSwapLimit(optional_idx limit) {
	throw NotImplementedException("This type of BufferManager can not set a swap limit");
}

vector<TemporaryFileInformation> BufferManager::GetTemporaryFiles() {
	throw InternalException("This type of BufferManager does not allow temporary files");
}

const string &BufferManager::GetTemporaryDirectory() const {
	throw InternalException("This type of BufferManager does not allow a temporary directory");
}

BufferPool &BufferManager::GetBufferPool() const {
	throw InternalException("This type of BufferManager does not have a buffer pool");
}

TemporaryMemoryManager &BufferManager::GetTemporaryMemoryManager() {
	throw NotImplementedException("This type of BufferManager does not have a TemporaryMemoryManager");
}

void BufferManager::SetTemporaryDirectory(const string &new_dir) {
	throw NotImplementedException("This type of BufferManager can not set a temporary directory");
}

bool BufferManager::HasTemporaryDirectory() const {
	return false;
}

//! Returns the maximum available memory for a given query
idx_t BufferManager::GetQueryMaxMemory() const {
	return GetBufferPool().GetQueryMaxMemory();
}

unique_ptr<FileBuffer> BufferManager::ConstructManagedBuffer(idx_t size, unique_ptr<FileBuffer> &&,
                                                             FileBufferType type) {
	throw NotImplementedException("This type of BufferManager can not construct managed buffers");
}

// Protected methods

void BufferManager::AddToEvictionQueue(shared_ptr<BlockHandle> &handle) {
	throw NotImplementedException("This type of BufferManager does not support 'AddToEvictionQueue");
}

void BufferManager::WriteTemporaryBuffer(MemoryTag tag, block_id_t block_id, FileBuffer &buffer) {
	throw NotImplementedException("This type of BufferManager does not support 'WriteTemporaryBuffer");
}

unique_ptr<FileBuffer> BufferManager::ReadTemporaryBuffer(MemoryTag tag, block_id_t id, unique_ptr<FileBuffer> buffer) {
	throw NotImplementedException("This type of BufferManager does not support 'ReadTemporaryBuffer");
}

void BufferManager::DeleteTemporaryFile(block_id_t id) {
	throw NotImplementedException("This type of BufferManager does not support 'DeleteTemporaryFile");
}

} // namespace duckdb
