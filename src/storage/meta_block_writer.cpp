#include "duckdb/storage/meta_block_writer.hpp"

#include <cstring>

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

void MetaBlockWriter::WriteData(const_data_ptr_t buffer, idx_t write_size) {
	while (offset + write_size > block->size) {
		// we need to make a new block
		// first copy what we can
		assert(offset <= block->size);
		idx_t copy_amount = block->size - offset;
		if (copy_amount > 0) {
			memcpy(block->buffer + offset, buffer, copy_amount);
			buffer += copy_amount;
			offset += copy_amount;
			write_size -= copy_amount;
		}
		// now we need to get a new block id
		block_id_t new_block_id = manager.GetFreeBlockId();
		// write the block id of the new block to the start of the current block
		*((block_id_t *)block->buffer) = new_block_id;
		// first flush the old block
		Flush();
		// now update the block id of the lbock
		block->id = new_block_id;
	}
	memcpy(block->buffer + offset, buffer, write_size);
	offset += write_size;
}
