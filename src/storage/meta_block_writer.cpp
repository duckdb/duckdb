#include "duckdb/storage/meta_block_writer.hpp"

#include <cstring>

namespace duckdb {

MetaBlockWriter::MetaBlockWriter(DatabaseInstance &db) : db(db) {
	auto &block_manager = BlockManager::GetBlockManager(db);
	block = block_manager.CreateBlock();
	Store<block_id_t>(-1, block->buffer);
	offset = sizeof(block_id_t);
}

MetaBlockWriter::~MetaBlockWriter() {
	Flush();
}

void MetaBlockWriter::Flush() {
	if (offset > sizeof(block_id_t)) {
		auto &block_manager = BlockManager::GetBlockManager(db);
		written_blocks.push_back(block->id);
		block_manager.Write(*block);
		offset = sizeof(block_id_t);
	}
}

void MetaBlockWriter::WriteData(const_data_ptr_t buffer, idx_t write_size) {
	while (offset + write_size > block->size) {
		// we need to make a new block
		// first copy what we can
		D_ASSERT(offset <= block->size);
		idx_t copy_amount = block->size - offset;
		if (copy_amount > 0) {
			memcpy(block->buffer + offset, buffer, copy_amount);
			buffer += copy_amount;
			offset += copy_amount;
			write_size -= copy_amount;
		}
		// now we need to get a new block id
		auto &block_manager = BlockManager::GetBlockManager(db);
		block_id_t new_block_id = block_manager.GetFreeBlockId();
		// write the block id of the new block to the start of the current block
		Store<block_id_t>(new_block_id, block->buffer);
		// first flush the old block
		Flush();
		// now update the block id of the lbock
		block->id = new_block_id;
		Store<block_id_t>(-1, block->buffer);
	}
	memcpy(block->buffer + offset, buffer, write_size);
	offset += write_size;
}

} // namespace duckdb
