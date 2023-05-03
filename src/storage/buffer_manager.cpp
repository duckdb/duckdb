#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/common/allocator.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_buffer.hpp"
#include "duckdb/storage/standard_buffer_manager.hpp"

namespace duckdb {

unique_ptr<BufferManager> BufferManager::CreateStandardBufferManager(DatabaseInstance &db, DBConfig &config) {
	return make_uniq<StandardBufferManager>(db, config.options.temporary_directory);
}

shared_ptr<BlockHandle> BufferManager::RegisterSmallMemory(idx_t block_size) {
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

void BufferManager::SetLimit(idx_t limit) {
	throw NotImplementedException("This type of BufferManager can not set a limit");
}

vector<TemporaryFileInformation> BufferManager::GetTemporaryFiles() {
	throw InternalException("This type of BufferManager does not allow temporary files");
}

const string &BufferManager::GetTemporaryDirectory() {
	throw InternalException("This type of BufferManager does not allow a temporary directory");
}

BufferPool &BufferManager::GetBufferPool() {
	throw InternalException("This type of BufferManager does not have a buffer pool");
}

void BufferManager::SetTemporaryDirectory(const string &new_dir) {
	throw NotImplementedException("This type of BufferManager can not set a temporary directory");
}

DatabaseInstance &BufferManager::GetDatabase() {
	throw NotImplementedException("This type of BufferManager is not linked to a DatabaseInstance");
}

bool BufferManager::HasTemporaryDirectory() const {
	return false;
}

unique_ptr<FileBuffer> BufferManager::ConstructManagedBuffer(idx_t size, unique_ptr<FileBuffer> &&source,
                                                             FileBufferType type) {
	throw NotImplementedException("This type of BufferManager can not construct managed buffers");
}

// Protected methods

void BufferManager::AddToEvictionQueue(shared_ptr<BlockHandle> &handle) {
	throw NotImplementedException("This type of BufferManager does not support 'AddToEvictionQueue");
}

void BufferManager::WriteTemporaryBuffer(block_id_t block_id, FileBuffer &buffer) {
	throw NotImplementedException("This type of BufferManager does not support 'WriteTemporaryBuffer");
}

unique_ptr<FileBuffer> BufferManager::ReadTemporaryBuffer(block_id_t id, unique_ptr<FileBuffer> buffer) {
	throw NotImplementedException("This type of BufferManager does not support 'ReadTemporaryBuffer");
}

void BufferManager::DeleteTemporaryFile(block_id_t id) {
	throw NotImplementedException("This type of BufferManager does not support 'DeleteTemporaryFile");
}

} // namespace duckdb
