#include "duckdb/storage/checkpoint/write_overflow_strings_to_disk.hpp"
#include "duckdb/storage/block_manager.hpp"
#include "duckdb/storage/buffer_manager.hpp"

namespace duckdb {

WriteOverflowStringsToDisk::WriteOverflowStringsToDisk(DatabaseInstance &db)
    : db(db), block_id(INVALID_BLOCK), offset(0) {
}

WriteOverflowStringsToDisk::~WriteOverflowStringsToDisk() {
	auto &block_manager = BlockManager::GetBlockManager(db);
	if (offset > 0) {
		block_manager.Write(*handle->node, block_id);
	}
}

void WriteOverflowStringsToDisk::WriteString(string_t string, block_id_t &result_block, int32_t &result_offset) {
	auto &buffer_manager = BufferManager::GetBufferManager(db);
	auto &block_manager = BlockManager::GetBlockManager(db);
	if (!handle) {
		handle = buffer_manager.Allocate(Storage::BLOCK_SIZE);
	}
	// first write the length of the string
	if (block_id == INVALID_BLOCK || offset + sizeof(uint32_t) >= STRING_SPACE) {
		AllocateNewBlock(block_manager.GetFreeBlockId());
	}
	result_block = block_id;
	result_offset = offset;

	// write the length field
	auto string_length = string.GetSize();
	Store<uint32_t>(string_length, handle->node->buffer + offset);
	offset += sizeof(uint32_t);
	// now write the remainder of the string
	auto strptr = string.GetDataUnsafe();
	uint32_t remaining = string_length;
	while (remaining > 0) {
		uint32_t to_write = MinValue<uint32_t>(remaining, STRING_SPACE - offset);
		if (to_write > 0) {
			memcpy(handle->node->buffer + offset, strptr, to_write);

			remaining -= to_write;
			offset += to_write;
			strptr += to_write;
		}
		if (remaining > 0) {
			// there is still remaining stuff to write
			// first get the new block id and write it to the end of the previous block
			auto new_block_id = block_manager.GetFreeBlockId();
			Store<block_id_t>(new_block_id, handle->node->buffer + offset);
			// now write the current block to disk and allocate a new block
			AllocateNewBlock(new_block_id);
		}
	}
}

void WriteOverflowStringsToDisk::AllocateNewBlock(block_id_t new_block_id) {
	auto &block_manager = BlockManager::GetBlockManager(db);
	if (block_id != INVALID_BLOCK) {
		// there is an old block, write it first
		block_manager.Write(*handle->node, block_id);
	}
	offset = 0;
	block_id = new_block_id;
}

} // namespace duckdb
