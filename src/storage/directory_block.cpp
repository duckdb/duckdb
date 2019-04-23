#include "storage/directory_block.hpp"
#include "common/fstream_util.hpp"

using namespace std;
using namespace duckdb;

void DirectoryBlock::Write(char *buffer, size_t buffered_size) {
	assert(buffered_size <= BLOCK_SIZE);
	fstream block_file;
	FstreamUtil::OpenFile(path, block_file, ios_base::out);
	block_file.write(buffer, buffered_size);
	FstreamUtil::CloseFile(block_file);
}

size_t DirectoryBlock::Read(char *buffer) {
	fstream block_file;
	FstreamUtil::OpenFile(path, block_file, ios_base::in);
	streamsize size = FstreamUtil::GetFileSize(block_file);
	block_file.seekg(0, ios::beg);
	assert(size <= BLOCK_SIZE);
	block_file.read(buffer, size);
	return size;
}

void DirectoryBlock::Read(char *buffer, size_t offset, size_t buffered_size) {
	fstream block_file;
	FstreamUtil::OpenFile(path, block_file, ios_base::in);
	block_file.seekg(offset, ios::beg);
	block_file.read(buffer, buffered_size);
	assert(block_file.good());
}
