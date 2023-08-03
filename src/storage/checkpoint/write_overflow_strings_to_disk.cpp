#include "duckdb/storage/checkpoint/write_overflow_strings_to_disk.hpp"
#include "duckdb/storage/block_manager.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "miniz_wrapper.hpp"

namespace duckdb {

WriteOverflowStringsToDisk::WriteOverflowStringsToDisk(BlockManager &block_manager)
    : block_manager(block_manager), block_id(INVALID_BLOCK), offset(0) {
}

WriteOverflowStringsToDisk::~WriteOverflowStringsToDisk() {
	if (offset > 0) {
		block_manager.Write(handle.GetFileBuffer(), block_id);
	}
}

void WriteOverflowStringsToDisk::WriteString(string_t string, block_id_t &result_block, int32_t &result_offset) {
	auto &buffer_manager = block_manager.buffer_manager;
	if (!handle.IsValid()) {
		handle = buffer_manager.Allocate(Storage::BLOCK_SIZE);
	}
	// first write the length of the string
	if (block_id == INVALID_BLOCK || offset + 2 * sizeof(uint32_t) >= STRING_SPACE) {
		AllocateNewBlock(block_manager.GetFreeBlockId());
	}
	result_block = block_id;
	result_offset = offset;

	// GZIP the string
	auto uncompressed_size = string.GetSize();
	MiniZStream s;
	size_t compressed_size = 0;
	compressed_size = s.MaxCompressedLength(uncompressed_size);
	auto compressed_buf = make_unsafe_uniq_array<data_t>(compressed_size);
	s.Compress(string.GetData(), uncompressed_size, char_ptr_cast(compressed_buf.get()), &compressed_size);
	string_t compressed_string(const_char_ptr_cast(compressed_buf.get()), compressed_size);

	// store sizes
	auto data_ptr = handle.Ptr();
	Store<uint32_t>(compressed_size, data_ptr + offset);
	Store<uint32_t>(uncompressed_size, data_ptr + offset + sizeof(uint32_t));

	// now write the remainder of the string
	offset += 2 * sizeof(uint32_t);
	auto strptr = compressed_string.GetData();
	uint32_t remaining = compressed_size;
	while (remaining > 0) {
		uint32_t to_write = MinValue<uint32_t>(remaining, STRING_SPACE - offset);
		if (to_write > 0) {
			memcpy(data_ptr + offset, strptr, to_write);

			remaining -= to_write;
			offset += to_write;
			strptr += to_write;
		}
		if (remaining > 0) {
			// there is still remaining stuff to write
			// first get the new block id and write it to the end of the previous block
			auto new_block_id = block_manager.GetFreeBlockId();
			Store<block_id_t>(new_block_id, data_ptr + offset);
			// now write the current block to disk and allocate a new block
			AllocateNewBlock(new_block_id);
		}
	}
}

void WriteOverflowStringsToDisk::AllocateNewBlock(block_id_t new_block_id) {
	if (block_id != INVALID_BLOCK) {
		// there is an old block, write it first
		block_manager.Write(handle.GetFileBuffer(), block_id);
	}
	offset = 0;
	block_id = new_block_id;
}

} // namespace duckdb
