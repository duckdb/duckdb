#include "duckdb/storage/meta_block_writer.hpp"

#include <cstring>

namespace duckdb {

MetaBlockWriter::MetaBlockWriter(BlockManager &block_manager, block_id_t initial_block_id)
    : block_manager(block_manager) {
	if (initial_block_id == INVALID_BLOCK) {
		initial_block_id = GetNextBlockId();
	}
	block = block_manager.CreateBlock(initial_block_id, nullptr);
	Store<block_id_t>(-1, block->buffer);
	offset = sizeof(block_id_t);
}

MetaBlockWriter::~MetaBlockWriter() {
	// If there's an exception during checkpoint, this can get destroyed without
	// flushing the data...which is fine, because none of the unwritten data
	// will be referenced.
	//
	// Otherwise, we should have explicitly flushed (and thereby nulled the block).
	D_ASSERT(!block || Exception::UncaughtException());
}

block_id_t MetaBlockWriter::GetNextBlockId() {
	return block_manager.GetFreeBlockId();
}

BlockPointer MetaBlockWriter::GetBlockPointer() {
	BlockPointer pointer;
	pointer.block_id = block->id;
	pointer.offset = offset;
	return pointer;
}

void MetaBlockWriter::Flush() {
	AdvanceBlock();
	block = nullptr;
}

void MetaBlockWriter::AdvanceBlock() {
	written_blocks.insert(block->id);
	if (offset > sizeof(block_id_t)) {
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
		block_id_t new_block_id = GetNextBlockId();
		// write the block id of the new block to the start of the current block
		Store<block_id_t>(new_block_id, block->buffer);
		// first flush the old block
		AdvanceBlock();
		// now update the block id of the block
		block->id = new_block_id;
		Store<block_id_t>(-1, block->buffer);
	}
	memcpy(block->buffer + offset, buffer, write_size);
	offset += write_size;
}

} // namespace duckdb
