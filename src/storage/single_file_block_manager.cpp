#include "storage/single_file_block_manager.hpp"

#include "common/exception.hpp"

using namespace duckdb;
using namespace std;


SingleFileBlockManager::SingleFileBlockManager(FileSystem &fs, string path, bool read_only, bool create_new) :
	path(path), header_buffer(HEADER_SIZE) {

	uint8_t flags;
	FileLockType lock;
	if (read_only) {
		assert(!create_new);
		flags = FileFlags::READ | FileFlags::DIRECT_IO;
		lock = FileLockType::READ_LOCK;
	} else {
		flags = FileFlags::WRITE | FileFlags::DIRECT_IO;
		lock = FileLockType::WRITE_LOCK;
		if (create_new) {
			flags |= FileFlags::CREATE;
		}
	}
	// open the RDBMS handle
	handle = fs.OpenFile(path, flags, lock);
	if (create_new) {
		// if we create a new file, we fill the metadata of the file
		// first fill in the new header
		header_buffer.Clear();
		MainHeader *main_header = (MainHeader*) header_buffer.buffer;
		main_header->version_number = VERSION_NUMBER;
		// now write the header to the file
		header_buffer.Write(*handle, 0);
		header_buffer.Clear();

		// write the database headers
		// we initialize meta_block and free_list to -1 because the database file does not contain any actual content yet
		DatabaseHeader *header = (DatabaseHeader*) header_buffer.buffer;
		// header 1
		header->iteration = 0;
		header->meta_block = -1;
		header->free_list = -1;
		header->block_count = 0;
		header_buffer.Write(*handle, HEADER_SIZE);
		// header 2
		header->iteration = 1;
		header_buffer.Write(*handle, HEADER_SIZE * 2);
		// ensure that writing to disk is completed before returning
		handle->Sync();
		// we start with h2 as active_header, this way our initial write will be in h1
		active_header = 1;
		max_block = 0;
	} else {
		MainHeader header;
		// otherwise, we check the metadata of the file
		header_buffer.Read(*handle, 0);
		header = *((MainHeader*) header_buffer.buffer);
		// check the version number
		if (header.version_number != VERSION_NUMBER) {
			throw IOException("Trying to read a database file with version number %lld, but we can only read version %lld", header.version_number, VERSION_NUMBER);
		}
		// read the database headers from disk
		DatabaseHeader h1, h2;
		header_buffer.Read(*handle, HEADER_SIZE);
		h1 = *((DatabaseHeader*) header_buffer.buffer);
		header_buffer.Read(*handle, HEADER_SIZE * 2);
		h2 = *((DatabaseHeader*) header_buffer.buffer);
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
	if (header.free_list >= 0) {
		// FIXME: load free_list
		// FIXME: initialize meta block pointer somewhere
	}
	meta_block = header.meta_block;
	iteration_count = header.iteration;
	max_block = header.block_count;
}

block_id_t SingleFileBlockManager::GetFreeBlockId() {
	if (free_list.size() > 0) {
		// free list is non empty
		// take an entry from the free list
		block_id_t block = free_list.back();
		// erase the entry from the free list again
		free_list.pop_back();
		return block;
	}
	return max_block++;
}

block_id_t SingleFileBlockManager::GetMetaBlock() {
	return meta_block;
}

unique_ptr<Block> SingleFileBlockManager::CreateBlock() {
	return make_unique<Block>(GetFreeBlockId());
}

void SingleFileBlockManager::Read(Block &block) {
	assert(block.id >= 0);
	block.Read(*handle, BLOCK_START + block.id * BLOCK_SIZE);
}

void SingleFileBlockManager::Write(Block &block) {
	assert(block.id >= 0);
	block.Write(*handle, BLOCK_START + block.id * BLOCK_SIZE);
}

void SingleFileBlockManager::WriteHeader(DatabaseHeader header) {
	// set the iteration count
	header.iteration = ++iteration_count;
	header.free_list = -1;
	header.block_count = max_block;
	// set the header inside the buffer
	header_buffer.Clear();
	*((DatabaseHeader*) header_buffer.buffer) = header;
	// now write the header to the file, active_header determines whether we write to h1 or h2
	// note that if active_header is h1 we write to h2, and vice versa
	header_buffer.Write(*handle, active_header == 1 ? HEADER_SIZE : HEADER_SIZE * 2);
	// switch active header to the other header
	active_header = 1 - active_header;
	//! Ensure the header write ends up on disk
	handle->Sync();
}
