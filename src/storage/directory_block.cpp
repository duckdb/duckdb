#include "storage/directory_block.hpp"

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
	std::streamsize size = FstreamUtil::GetFileSize(block_file);
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

bool DirectoryBlock::HasNoSpace(DataChunk &chunk) {
	auto size_next_chunk = 0;
	auto tuples_count = chunk.size();
	size_t data_length = 0;

	// calculate the necessary space for the given chunk
	for (size_t col_idx = 0; col_idx < chunk.column_count; col_idx++) {
		auto &vec = chunk.data[col_idx];
		auto type = chunk.data[col_idx].type;
		if (TypeIsConstantSize(type)) {
			data_length = (GetTypeIdSize(type) * vec.count);
		} else {
			assert(type == TypeId::VARCHAR);
			// strings are inlined into the block
			// we use null-padding to store them
			const char **strings = (const char **)vec.data;
			VectorOperations::Exec(vec, [&](size_t i, size_t k) {
				auto source = vec.nullmask[i] ? strings[k] : NullValue<const char *>();
				size_t str_length = strlen(source);
				data_length += str_length;
			});
		}
		size_next_chunk += data_length;
	}
	return ((offset + size_next_chunk) > BLOCK_SIZE);
}