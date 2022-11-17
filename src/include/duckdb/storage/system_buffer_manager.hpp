//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/buffer_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/buffer_manager.hpp"
//#include "duckdb/common/atomic.hpp"
//#include "duckdb/common/file_system.hpp"
//#include "duckdb/common/mutex.hpp"
//#include "duckdb/common/unordered_map.hpp"
//#include "duckdb/storage/block_manager.hpp"
//#include "duckdb/storage/buffer/block_handle.hpp"
//#include "duckdb/storage/buffer/buffer_handle.hpp"
//#include "duckdb/common/allocator.hpp"

namespace duckdb {
class BlockManager;
class DatabaseInstance;
class TemporaryDirectoryHandle;
struct EvictionQueue;

//! The SystemBufferManager is a concrete implementation of BufferManager that is responsible for
//! all the memory management of the system by default
class SystemBufferManager : public BufferManager {
	friend class BufferHandle;
	friend class BlockHandle;
	friend class BlockManager;

public:
	SystemBufferManager(DatabaseInstance &db, string temp_directory, idx_t maximum_memory);
	virtual ~SystemBufferManager();

private:
};

} // namespace duckdb
