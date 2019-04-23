#include "storage/directory_block_manager.hpp"
#include "storage/meta_block_writer.hpp"
#include "common/file_system.hpp"
#include "common/exception.hpp"
#include "common/fstream_util.hpp"
#include "common/serializer.hpp"

using namespace std;
using namespace duckdb;

string DirectoryBlockManager::GetBlockPath(block_id_t id) {
	return JoinPath(directory_path, to_string(id) + ".duck");
}

unique_ptr<Block> DirectoryBlockManager::GetBlock(block_id_t id) {
	// load an existing block
	// the file should exist, if it doesn't we throw an exception
	auto block_path = GetBlockPath(id);
	return make_unique<DirectoryBlock>(id, block_path);
}

unique_ptr<Block> DirectoryBlockManager::CreateBlock() {
	while (FileExists(GetBlockPath(free_id))) {
		free_id++;
	}
	auto block = make_unique<DirectoryBlock>(free_id, GetBlockPath(free_id));
	free_id++;
	return move(block);
}

void DirectoryBlockManager::Flush(unique_ptr<Block> &block) {
	assert(block->offset > 0);
	// the amount of data to be flushed
	auto buffered_size = block->offset;
	// delegates the writing to block object
	block->Write(data_buffer.get(), buffered_size);
	// calls deleter for the old buffer and reset it
	data_buffer.reset(new char[BLOCK_SIZE]);
}


void DirectoryBlockManager::WriteHeader(int64_t version, block_id_t meta_block) {
	string header_path = JoinPath(directory_path, "header.duck");

	Serializer buffer;
	buffer.Write<int64_t>(version);
	buffer.Write<block_id_t>(meta_block);
	auto data = buffer.GetData();

	fstream header_file;
	FstreamUtil::OpenFile(header_path, header_file, ios_base::out);
	header_file.write((char*) data.data.get(), data.size);
	FstreamUtil::CloseFile(header_file);
}
