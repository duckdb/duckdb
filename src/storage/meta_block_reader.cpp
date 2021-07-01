#include "duckdb/storage/meta_block_reader.hpp"
#include "duckdb/storage/buffer_manager.hpp"

#include <cstring>

namespace duckdb {

MetaBlockReader::MetaBlockReader(DatabaseInstance &db, block_id_t block_id)
    : db(db), handle(nullptr), offset(0), next_block(-1) {
	ReadNewBlock(block_id);
}

MetaBlockReader::~MetaBlockReader() {
}

void MetaBlockReader::ReadData(data_ptr_t buffer, idx_t read_size) {
	while (offset + read_size > handle->node->size) {
		// cannot read entire entry from block
		// first read what we can from this block
		idx_t to_read = handle->node->size - offset;
		if (to_read > 0) {
			memcpy(buffer, handle->node->buffer + offset, to_read);
			read_size -= to_read;
			buffer += to_read;
		}
		// then move to the next block
		ReadNewBlock(next_block);
	}
	// we have enough left in this block to read from the buffer
	memcpy(buffer, handle->node->buffer + offset, read_size);
	offset += read_size;
}

void MetaBlockReader::ReadNewBlock(block_id_t id) {
	auto &block_manager = BlockManager::GetBlockManager(db);
	auto &buffer_manager = BufferManager::GetBufferManager(db);

	block_manager.MarkBlockAsModified(id);
	block = buffer_manager.RegisterBlock(id);
	handle = buffer_manager.Pin(block);

	next_block = Load<block_id_t>(handle->node->buffer);
	D_ASSERT(next_block >= -1);
	offset = sizeof(block_id_t);
}

} // namespace duckdb
