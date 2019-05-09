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

enum class StorageLockType { SHARED = 0, EXCLUSIVE = 1 };

class StorageLock {
	friend class StorageChunk;

public:
	~StorageLock();

private:
	StorageLock(StorageChunk *chunk, StorageLockType type) : type(type), chunk(chunk) {
	}

	StorageLockType type;
	StorageChunk *chunk;
};

class StorageChunk {
	friend class StorageLock;

public:
	StorageChunk(DataTable &table, uint64_t start);

	DataTable &table;
	bool deleted[STORAGE_CHUNK_SIZE] = {0};
	VersionInformation *version_pointers[STORAGE_CHUNK_SIZE] = {nullptr};
	vector<char *> columns;
	uint64_t count;
	uint64_t start;

	// Cleanup the version information of a tuple
	void Cleanup(VersionInformation *info);
	// Undo the changes made by a tuple
	void Undo(VersionInformation *info);

	//! Get an exclusive lock
	unique_ptr<StorageLock> GetExclusiveLock();
	//! Get a shared lock on the chunk
	unique_ptr<StorageLock> GetSharedLock();

	unique_ptr<StorageChunk> next;
	StringHeap string_heap;

private:
	unique_ptr<char[]> owned_data;
	std::mutex exclusive_lock;
	std::atomic<uint64_t> read_count;

	//! Release an exclusive lock on the chunk
	void ReleaseExclusiveLock();
	//! Release a shared lock on the chunk
	void ReleaseSharedLock();
};

} // namespace duckdb
