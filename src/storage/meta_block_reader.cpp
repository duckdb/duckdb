#include "duckdb/storage/meta_block_reader.hpp"

#include <cstring>

using namespace duckdb;
using namespace std;

MetaBlockReader::MetaBlockReader(BufferManager &manager, block_id_t block_id)
    : manager(manager), handle(nullptr), offset(0), next_block(-1) {
	ReadNewBlock(block_id);
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
	handle = manager.Pin(id);

	next_block = *((block_id_t *)handle->node->buffer);
	offset = sizeof(block_id_t);
}
