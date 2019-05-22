//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/storage_chunk.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/types/string_heap.hpp"
#include "storage/storage_lock.hpp"

namespace duckdb {
class ColumnDefinition;
class DataTable;
class StorageManager;
class StorageChunk;

struct VersionInformation;

class StorageChunk {
public:
	StorageChunk(DataTable &table, index_t start);

	DataTable &table;
	bool deleted[STORAGE_CHUNK_SIZE] = {0};
	VersionInformation *version_pointers[STORAGE_CHUNK_SIZE] = {nullptr};
	vector<data_ptr_t> columns;
	index_t count;
	index_t start;

	// Cleanup the version information of a tuple
	void Cleanup(VersionInformation *info);
	// Undo the changes made by a tuple
	void Undo(VersionInformation *info);

	StorageLock lock;
	unique_ptr<StorageChunk> next;
	StringHeap string_heap;

private:
	unique_ptr<data_t[]> owned_data;
};

} // namespace duckdb
