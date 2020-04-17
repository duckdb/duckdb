#include "duckdb/storage/single_file_block_manager.hpp"
#include "duckdb/storage/meta_block_writer.hpp"
#include "duckdb/storage/meta_block_reader.hpp"
#include "duckdb/common/exception.hpp"

using namespace duckdb;
using namespace std;

SingleFileBlockManager::SingleFileBlockManager(FileSystem &fs, string path, bool read_only, bool create_new,
                                               bool use_direct_io)
    : path(path), header_buffer(FileBufferType::MANAGED_BUFFER, Storage::FILE_HEADER_SIZE), read_only(read_only),
      use_direct_io(use_direct_io) {

	uint8_t flags;
	FileLockType lock;
	if (read_only) {
		assert(!create_new);
		flags = FileFlags::READ;
		lock = FileLockType::READ_LOCK;
	} else {
		flags = FileFlags::WRITE;
		lock = FileLockType::WRITE_LOCK;
		if (create_new) {
			flags |= FileFlags::CREATE;
		}
	}
	if (use_direct_io) {
		flags |= FileFlags::DIRECT_IO;
	}
	// open the RDBMS handle
	handle = fs.OpenFile(path, flags, lock);
	if (create_new) {
		// if we create a new file, we fill the metadata of the file
		// first fill in the new header
		header_buffer.Clear();
		MainHeader *main_header = (MainHeader *)header_buffer.buffer;
		main_header->version_number = VERSION_NUMBER;
		// now write the header to the file
		header_buffer.Write(*handle, 0);
		header_buffer.Clear();

		// write the database headers
		// initialize meta_block and free_list to INVALID_BLOCK because the database file does not contain any actual
		// content yet
		DatabaseHeader *header = (DatabaseHeader *)header_buffer.buffer;
		// header 1
		header->iteration = 0;
		header->meta_block = INVALID_BLOCK;
		header->free_list = INVALID_BLOCK;
		header->block_count = 0;
		header_buffer.Write(*handle, Storage::FILE_HEADER_SIZE);
		// header 2
		header->iteration = 1;
		header_buffer.Write(*handle, Storage::FILE_HEADER_SIZE * 2);
		// ensure that writing to disk is completed before returning
		handle->Sync();
		// we start with h2 as active_header, this way our initial write will be in h1
		active_header = 1;
		max_block = 0;
	} else {
		MainHeader header;
		// otherwise, we check the metadata of the file
		header_buffer.Read(*handle, 0);
		header = *((MainHeader *)header_buffer.buffer);
		// check the version number
		if (header.version_number != VERSION_NUMBER) {
			throw IOException(
			    "Trying to read a database file with version number %lld, but we can only read version %lld",
			    header.version_number, VERSION_NUMBER);
		}
		// read the database headers from disk
		DatabaseHeader h1, h2;
		header_buffer.Read(*handle, Storage::FILE_HEADER_SIZE);
		h1 = *((DatabaseHeader *)header_buffer.buffer);
		header_buffer.Read(*handle, Storage::FILE_HEADER_SIZE * 2);
		h2 = *((DatabaseHeader *)header_buffer.buffer);
		// check the header with the highest iteration count
		if (h1.iteration > h2.iteration) {
			// h1 is active header
			active_header = 0;
			Initialize(h1);
		} else {
			// h2 is active header
			active_header = 1;
			Initialize(h2);
		}
	}
}

void SingleFileBlockManager::Initialize(DatabaseHeader &header) {
	free_list_id = header.free_list;
	meta_block = header.meta_block;
	iteration_count = header.iteration;
	max_block = header.block_count;
}

void SingleFileBlockManager::LoadFreeList(BufferManager &manager) {
	if (read_only) {
		// no need to load free list for read only db
		return;
	}
	if (free_list_id == INVALID_BLOCK) {
		// no free list
		return;
	}
	MetaBlockReader reader(manager, free_list_id);
	auto free_list_count = reader.Read<uint64_t>();
	free_list.clear();
	free_list.reserve(free_list_count);
	for (idx_t i = 0; i < free_list_count; i++) {
		free_list.push_back(reader.Read<block_id_t>());
	}
}

void SingleFileBlockManager::StartCheckpoint() {
	used_blocks.clear();
}

block_id_t SingleFileBlockManager::GetFreeBlockId() {
	block_id_t block;
	if (free_list.size() > 0) {
		// free list is non empty
		// take an entry from the free list
		block = free_list.back();
		// erase the entry from the free list again
		free_list.pop_back();
	} else {
		block = max_block++;
	}
	used_blocks.insert(block);
	return block;
}

block_id_t SingleFileBlockManager::GetMetaBlock() {
	return meta_block;
}

unique_ptr<Block> SingleFileBlockManager::CreateBlock() {
	return make_unique<Block>(GetFreeBlockId());
}

void SingleFileBlockManager::Read(Block &block) {
	assert(block.id >= 0);
	assert(std::find(free_list.begin(), free_list.end(), block.id) == free_list.end());
	block.Read(*handle, BLOCK_START + block.id * Storage::BLOCK_ALLOC_SIZE);
}

void SingleFileBlockManager::Write(FileBuffer &buffer, block_id_t block_id) {
	assert(block_id >= 0);
	buffer.Write(*handle, BLOCK_START + block_id * Storage::BLOCK_ALLOC_SIZE);
}

void SingleFileBlockManager::WriteHeader(DatabaseHeader header) {
	// set the iteration count
	header.iteration = ++iteration_count;
	header.block_count = max_block;
	// now handle the free list
	free_list.clear();
	for (block_id_t i = 0; i < max_block; i++) {
		if (used_blocks.find(i) == used_blocks.end()) {
			free_list.push_back(i);
		}
	}
	if (free_list.size() > 0) {
		// there are blocks in the free list
		// write them to the file
		MetaBlockWriter writer(*this);
		auto entry = std::find(free_list.begin(), free_list.end(), writer.block->id);
		if (entry != free_list.end()) {
			free_list.erase(entry);
		}
		header.free_list = writer.block->id;

		writer.Write<uint64_t>(free_list.size());
		for (auto &block_id : free_list) {
			writer.Write<block_id_t>(block_id);
		}
		writer.Flush();
	} else {
		// no blocks in the free list
		header.free_list = INVALID_BLOCK;
	}
	if (!use_direct_io) {
		// if we are not using Direct IO we need to fsync BEFORE we write the header to ensure that all the previous
		// blocks are written as well
		handle->Sync();
	}
	// set the header inside the buffer
	header_buffer.Clear();
	*((DatabaseHeader *)header_buffer.buffer) = header;
	// now write the header to the file, active_header determines whether we write to h1 or h2
	// note that if active_header is h1 we write to h2, and vice versa
	header_buffer.Write(*handle, active_header == 1 ? Storage::FILE_HEADER_SIZE : Storage::FILE_HEADER_SIZE * 2);
	// switch active header to the other header
	active_header = 1 - active_header;
	//! Ensure the header write ends up on disk
	handle->Sync();

	// the free list is now equal to the blocks that were used by the previous iteration
	free_list.clear();
	for (auto &block_id : used_blocks) {
		free_list.push_back(block_id);
	}
	used_blocks.clear();
}
