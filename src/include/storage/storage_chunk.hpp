//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/storage_chunk.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/types/string_heap.hpp"

#include <atomic>
#include <mutex>
#include <vector>

namespace duckdb {
class ColumnDefinition;
class DataTable;
class StorageManager;
class StorageChunk;

struct VersionInformation;

class ExclusiveStorageChunkLock {
public:
	ExclusiveStorageChunkLock(StorageChunk* chunk) : chunk(chunk) { }
	~ExclusiveStorageChunkLock();
private:
	StorageChunk* chunk;
};

class StorageChunk {
	friend class ExclusiveStorageChunkLock;
public:
	StorageChunk(DataTable &table, size_t start);
	
	DataTable &table;
	bool deleted[STORAGE_CHUNK_SIZE] = {0};
	VersionInformation *version_pointers[STORAGE_CHUNK_SIZE] = {nullptr};
	vector<char *> columns;
	size_t count;
	size_t start;

	// Cleanup the version information of a tuple
	void Cleanup(VersionInformation *info);
	// Undo the changes made by a tuple
	void Undo(VersionInformation *info);

	//! Get an exclusive lock
	unique_ptr<ExclusiveStorageChunkLock> GetExclusiveLock();
	//! Get a shared lock on the chunk
	void GetSharedLock();
	//! Release a shared lock on the chunk
	void ReleaseSharedLock();
	
	unique_ptr<StorageChunk> next;
	StringHeap string_heap;

private:
	unique_ptr<char[]> owned_data;
	std::mutex exclusive_lock;
	std::atomic<size_t> read_count;

	//! Release an exclusive lock on the chunk
	void ReleaseExclusiveLock();
};

} // namespace duckdb
