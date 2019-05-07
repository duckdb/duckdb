#include "storage/single_file_block_manager.hpp"

#include "common/exception.hpp"
#include "storage/storage_info.hpp"

#include <unistd.h>
#include <fcntl.h>

using namespace duckdb;
using namespace std;


SingleFileBlockManager::SingleFileBlockManager(string path, bool read_only, bool create_new) :
	path(path) {

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
	handle = FileSystem::OpenFile(path, flags, lock);
	// create a temporary buffer for reads/writes
	header_buffer = FileBuffer::AllocateAlignedBuffer(HEADER_SIZE);
	if (create_new) {
		// if we create a new file, we fill the metadata of the file
		// first fill in the new header
		header_buffer->Clear();
		MainHeader *main_header = (MainHeader*) header_buffer->buffer;
		main_header->version_number = VERSION_NUMBER;
		// now write the header to the file
		header_buffer->Write(*handle, 0);
		header_buffer->Clear();

		// write the database headers
		// we initialize meta_block and free_list to -1 because the database file does not contain any actual content yet
		DatabaseHeader *header = (DatabaseHeader*) header_buffer->buffer;
		// header 1
		header->iteration = 0;
		header->meta_block = -1;
		header->free_list = -1;
		header_buffer->Write(*handle, HEADER_SIZE);
		// header 2
		header->iteration = 1;
		header->meta_block = -1;
		header->free_list = -1;
		header_buffer->Write(*handle, HEADER_SIZE * 2);
	} else {
		MainHeader header;
		// otherwise, we check the metadata of the file
		header_buffer->Read(*handle, 0);
		header = *((MainHeader*) header_buffer->buffer);
		// check the version number
		if (header.version_number != VERSION_NUMBER) {
			throw IOException("Trying to read a database file with version number %lld, but we can only read version %lld", header.version_number, VERSION_NUMBER);
		}
		// read the database headers from disk
		DatabaseHeader h1, h2;
		header_buffer->Read(*handle, HEADER_SIZE);
		h1 = *((DatabaseHeader*) header_buffer->buffer);
		header_buffer->Read(*handle, HEADER_SIZE * 2);
		h2 = *((DatabaseHeader*) header_buffer->buffer);
		// check the header with the highest iteration count
		if (h1.iteration > h2.iteration) {
			// FIXME: use h1
		} else {
			// FIXME: use h2
		}
	}
}

unique_ptr<Block> SingleFileBlockManager::GetBlock(block_id_t id) {
	return nullptr;
}

string SingleFileBlockManager::GetBlockPath(block_id_t id) {
	return "";
}

unique_ptr<Block> SingleFileBlockManager::CreateBlock() {
	return nullptr;
}

void SingleFileBlockManager::Flush(unique_ptr<Block> &block) {

}

void SingleFileBlockManager::WriteHeader(int64_t version, block_id_t meta_block) {

}
