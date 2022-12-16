#include "duckdb/storage/meta_block_reader.hpp"
#include "duckdb/storage/buffer_manager.hpp"

#include <cstring>

namespace duckdb {

MetaBlockReader::MetaBlockReader(BlockManager &block_manager, block_id_t block_id, bool free_blocks_on_read)
    : block_manager(block_manager), offset(0), next_block(-1), free_blocks_on_read(free_blocks_on_read) {
	ReadNewBlock(block_id);
}

MetaBlockReader::~MetaBlockReader() {
}

void MetaBlockReader::ReadData(data_ptr_t buffer, idx_t read_size) {
	while (offset + read_size > handle.GetFileBuffer().size) {
		// cannot read entire entry from block
		// first read what we can from this block
		idx_t to_read = handle.GetFileBuffer().size - offset;
		if (to_read > 0) {
			memcpy(buffer, handle.Ptr() + offset, to_read);
			read_size -= to_read;
			buffer += to_read;
		}
		// then move to the next block
		if (next_block == INVALID_BLOCK) {
			throw IOException("Cannot read from INVALID_BLOCK.");
		}
		ReadNewBlock(next_block);
	}
	// we have enough left in this block to read from the buffer
	memcpy(buffer, handle.Ptr() + offset, read_size);
	offset += read_size;
}

void MetaBlockReader::ReadNewBlock(block_id_t id) {
	auto &buffer_manager = block_manager.buffer_manager;

	// Marking these blocks as modified will cause them to be moved to the free
	// list upon the next successful checkpoint. Marking them modified here
	// assumes MetaBlockReader is exclusively used for reading checkpoint data,
	// and thus any blocks we're reading will be obviated by the next checkpoint.
	if (free_blocks_on_read) {
		block_manager.MarkBlockAsModified(id);
	}
	block = block_manager.RegisterBlock(id, true);
	handle = buffer_manager.Pin(block);

	next_block = Load<block_id_t>(handle.Ptr());
	D_ASSERT(next_block >= -1);
	offset = sizeof(block_id_t);
}

} // namespace duckdb
