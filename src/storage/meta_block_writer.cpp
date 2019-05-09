#include "storage/meta_block_writer.hpp"

using namespace duckdb;
using namespace std;

MetaBlockWriter::MetaBlockWriter(BlockManager &manager) : manager(manager) {
	block = manager.CreateBlock();
	offset = sizeof(block_id_t);
}

MetaBlockWriter::~MetaBlockWriter() {
	Flush();
}

void MetaBlockWriter::Flush() {
	if (offset > sizeof(block_id_t)) {
		manager.Write(*block);
		offset = sizeof(block_id_t);
	}
}

void MetaBlockWriter::Write(const char *data, uint64_t data_size) {
	while (offset + data_size > block->size) {
		// we need to make a new block
		// first copy what we can
		int64_t copy_amount = block->size - offset;
		if (copy_amount > 0) {
			assert(copy_amount < data_size);
			memcpy(block->buffer + offset, data, copy_amount);
			data += copy_amount;
			data_size -= copy_amount;
		}
		// now we need to get a new block id
		block_id_t new_block_id = manager.GetFreeBlockId();
		// write the block id of the new block to the start of the current block
		memcpy(block->buffer, &new_block_id, sizeof(block_id_t));
		// first flush the old block
		Flush();
		// now update the block id of the lbock
		block->id = new_block_id;
	}
	memcpy(block->buffer + offset, data, data_size);
	offset += data_size;
}
