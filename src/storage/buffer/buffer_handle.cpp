#include "duckdb/storage/buffer/buffer_handle.hpp"
#include "duckdb/storage/buffer_manager.hpp"

using namespace duckdb;
using namespace std;

BufferHandle::BufferHandle(BufferManager &manager, block_id_t block_id, FileBuffer *node)
    : manager(manager), block_id(block_id), node(node) {
}

BufferHandle::~BufferHandle() {
	manager.Unpin(block_id);
}
