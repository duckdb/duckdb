#include "duckdb/storage/checkpoint/write_overflow_strings_to_disk.hpp"
#include "duckdb/storage/block_manager.hpp"
#include "duckdb/storage/buffer_manager.hpp"

namespace duckdb {

WriteOverflowStringsToDisk::WriteOverflowStringsToDisk(BlockManager &block_manager)
    : block_manager(block_manager), block_id(INVALID_BLOCK), offset(0) {
}

WriteOverflowStringsToDisk::~WriteOverflowStringsToDisk() {
	// verify that the overflow writer has been flushed
	D_ASSERT(Exception::UncaughtException() || offset == 0);
}

shared_ptr<BlockHandle> UncompressedStringSegmentState::GetHandle(BlockManager &manager, block_id_t block_id) {
	lock_guard<mutex> lock(block_lock);
	auto entry = handles.find(block_id);
	if (entry != handles.end()) {
		return entry->second;
	}
	auto result = manager.RegisterBlock(block_id);
	handles.insert(make_pair(block_id, result));
	return result;
}

void UncompressedStringSegmentState::RegisterBlock(BlockManager &manager, block_id_t block_id) {
	lock_guard<mutex> lock(block_lock);
	auto entry = handles.find(block_id);
	if (entry != handles.end()) {
		throw InternalException("UncompressedStringSegmentState::RegisterBlock - block id %llu already exists",
		                        block_id);
	}
	auto result = manager.RegisterBlock(block_id);
	handles.insert(make_pair(block_id, std::move(result)));
	on_disk_blocks.push_back(block_id);
}

void WriteOverflowStringsToDisk::WriteString(UncompressedStringSegmentState &state, string_t string,
                                             block_id_t &result_block, int32_t &result_offset) {
	auto &buffer_manager = block_manager.buffer_manager;
	if (!handle.IsValid()) {
		handle = buffer_manager.Allocate(Storage::BLOCK_SIZE);
	}
	// first write the length of the string
	if (block_id == INVALID_BLOCK || offset + 2 * sizeof(uint32_t) >= STRING_SPACE) {
		AllocateNewBlock(state, block_manager.GetFreeBlockId());
	}
	result_block = block_id;
	result_offset = offset;

	// write the length field
	auto data_ptr = handle.Ptr();
	auto string_length = string.GetSize();
	Store<uint32_t>(string_length, data_ptr + offset);
	offset += sizeof(uint32_t);

	// now write the remainder of the string
	auto strptr = string.GetData();
	uint32_t remaining = string_length;
	while (remaining > 0) {
		uint32_t to_write = MinValue<uint32_t>(remaining, STRING_SPACE - offset);
		if (to_write > 0) {
			memcpy(data_ptr + offset, strptr, to_write);

			remaining -= to_write;
			offset += to_write;
			strptr += to_write;
		}
		if (remaining > 0) {
			D_ASSERT(offset == WriteOverflowStringsToDisk::STRING_SPACE);
			// there is still remaining stuff to write
			// now write the current block to disk and allocate a new block
			AllocateNewBlock(state, block_manager.GetFreeBlockId());
		}
	}
}

void WriteOverflowStringsToDisk::Flush() {
	if (block_id != INVALID_BLOCK && offset > 0) {
		// zero-initialize the empty part of the overflow string buffer (if any)
		if (offset < STRING_SPACE) {
			memset(handle.Ptr() + offset, 0, STRING_SPACE - offset);
		}
		// write to disk
		block_manager.Write(handle.GetFileBuffer(), block_id);
	}
	block_id = INVALID_BLOCK;
	offset = 0;
}

void WriteOverflowStringsToDisk::AllocateNewBlock(UncompressedStringSegmentState &state, block_id_t new_block_id) {
	if (block_id != INVALID_BLOCK) {
		// there is an old block, write it first
		// write the new block id at the end of the previous block
		Store<block_id_t>(new_block_id, handle.Ptr() + WriteOverflowStringsToDisk::STRING_SPACE);
		Flush();
	}
	offset = 0;
	block_id = new_block_id;
	state.RegisterBlock(block_manager, new_block_id);
}

} // namespace duckdb
