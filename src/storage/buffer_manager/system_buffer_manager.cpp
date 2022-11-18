#include "duckdb/storage/system_buffer_manager.hpp"
#include "duckdb/storage/buffer_manager.hpp"

#include "duckdb/common/allocator.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/set.hpp"
#include "duckdb/parallel/concurrentqueue.hpp"
#include "duckdb/storage/in_memory_block_manager.hpp"
#include "duckdb/storage/storage_manager.hpp"

namespace duckdb {

SystemBufferManager::SystemBufferManager(DatabaseInstance &db, string tmp, idx_t maximum_memory)
    : BufferManager(db, tmp, maximum_memory) {
}

SystemBufferManager::~SystemBufferManager() {
}

} // namespace duckdb
