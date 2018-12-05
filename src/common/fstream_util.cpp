#include "common/fstream_util.hpp"

using namespace std;
using namespace duckdb;

fstream FstreamUtil::OpenFile(const string &file_path) {
	fstream new_file;
	new_file.open(file_path, ios::in | ios::out | ios::binary);
	if (!new_file.good()) {
		throw IOException("Could not open File!");
	}

	return new_file;
}

size_t FstreamUtil::GetFileSize(fstream &file) {
	file.seekg(0, ios::end);
	return file.tellg();
}

unique_ptr<char[]> FstreamUtil::ReadBinary(fstream &file) {
	size_t file_size = GetFileSize(file);
	file.seekg(0, ios::beg);
	auto result = unique_ptr<char[]>(new char[file_size]);
	file.read(result.get(), file_size);

	return result;
}
