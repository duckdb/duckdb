#include "storage/meta_block_reader.hpp"

using namespace duckdb;
using namespace std;

MetaBlockReader::MetaBlockReader(BlockManager &manager, block_id_t block_id)
    : manager(manager), block(make_unique<Block>(-1)), offset(0), next_block(-1) {
	ReadNewBlock(block_id);
}

void MetaBlockReader::ReadData(data_ptr_t buffer, index_t read_size) {
	while (offset + read_size > block->size) {
		// cannot read entire entry from block
		// first read what we can from this block
		index_t to_read = block->size - offset;
		if (to_read > 0) {
			memcpy(buffer, block->buffer + offset, to_read);
			read_size -= to_read;
			buffer += to_read;
		}
		// then move to the next block
		ReadNewBlock(next_block);
	}
	// we have enough left in this block to read from the buffer
	memcpy(buffer, block->buffer + offset, read_size);
	offset += read_size;
}

void MetaBlockReader::ReadNewBlock(block_id_t id) {
	block->id = id;
	manager.Read(*block);
	next_block = *((block_id_t *)block->buffer);
	offset = sizeof(block_id_t);
}
