#include "duckdb/storage/buffer_manager.hpp"

#include "duckdb/common/allocator.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_buffer.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/storage/buffer/buffer_pool.hpp"
#include "duckdb/storage/standard_buffer_manager.hpp"

namespace duckdb {

// Static functions.

BufferManager &BufferManager::GetBufferManager(DatabaseInstance &db) {
	return db.GetBufferManager();
}

const BufferManager &BufferManager::GetBufferManager(const DatabaseInstance &db) {
	return db.GetBufferManager();
}

BufferManager &BufferManager::GetBufferManager(ClientContext &context) {
	auto &client_data = ClientData::Get(context);
	return *client_data.client_buffer_manager;
}

const BufferManager &BufferManager::GetBufferManager(const ClientContext &context) {
	auto &client_data = ClientData::Get(context);
	return *client_data.client_buffer_manager;
}

BufferManager &BufferManager::GetBufferManager(AttachedDatabase &db) {
	return BufferManager::GetBufferManager(db.GetDatabase());
}

idx_t BufferManager::GetAllocSize(const idx_t alloc_size) {
	return AlignValue<idx_t, Storage::SECTOR_SIZE>(alloc_size);
}

// Virtual functions.

shared_ptr<BlockHandle> BufferManager::RegisterTransientMemory(const idx_t size, BlockManager &block_manager) {
	throw NotImplementedException("This type of BufferManager can not create 'transient-memory' blocks");
}

shared_ptr<BlockHandle> BufferManager::RegisterSmallMemory(const idx_t size) {
	return RegisterSmallMemory(MemoryTag::BASE_TABLE, size);
}

shared_ptr<BlockHandle> BufferManager::RegisterSmallMemory(MemoryTag tag, const idx_t size) {
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

void BufferManager::SetTemporaryDirectory(const string &new_dir) {
	throw NotImplementedException("This type of BufferManager can not set a temporary directory");
}

bool BufferManager::HasTemporaryDirectory() const {
	return false;
}

unique_ptr<FileBuffer> BufferManager::ConstructManagedBuffer(idx_t size, idx_t block_header_size,
                                                             unique_ptr<FileBuffer> &&, FileBufferType type) {
	throw NotImplementedException("This type of BufferManager can not construct managed buffers");
}

BufferPool &BufferManager::GetBufferPool() const {
	throw InternalException("This type of BufferManager does not have a buffer pool");
}

TemporaryMemoryManager &BufferManager::GetTemporaryMemoryManager() {
	throw NotImplementedException("This type of BufferManager does not have a TemporaryMemoryManager");
}

void BufferManager::AddToEvictionQueue(shared_ptr<BlockHandle> &handle) {
	throw NotImplementedException("This type of BufferManager does not support 'AddToEvictionQueue");
}

void BufferManager::WriteTemporaryBuffer(MemoryTag tag, block_id_t block_id, FileBuffer &buffer) {
	throw NotImplementedException("This type of BufferManager does not support 'WriteTemporaryBuffer");
}

unique_ptr<FileBuffer> BufferManager::ReadTemporaryBuffer(MemoryTag tag, BlockHandle &block,
                                                          unique_ptr<FileBuffer> buffer) {
	throw NotImplementedException("This type of BufferManager does not support 'ReadTemporaryBuffer");
}

void BufferManager::DeleteTemporaryFile(BlockHandle &block) {
	throw NotImplementedException("This type of BufferManager does not support 'DeleteTemporaryFile");
}

} // namespace duckdb
