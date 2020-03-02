#include "duckdb/common/fstream_util.hpp"

using namespace std;
using namespace duckdb;

void FstreamUtil::OpenFile(const string &file_path, fstream &new_file, ios_base::openmode mode) {
	new_file.open(file_path, mode);
	if (!new_file.good()) {
		throw IOException("Could not open File!" + file_path);
	}
}

void FstreamUtil::CloseFile(fstream &file) {
	file.close();
	// check the success of the write
	if (file.fail()) {
		throw IOException("Failed to close the file!");
	}
}

idx_t FstreamUtil::GetFileSize(fstream &file) {
	file.seekg(0, ios::end);
	return file.tellg();
}

data_ptr FstreamUtil::ReadBinary(fstream &file) {
	auto file_size = GetFileSize(file);
	file.seekg(0, ios::beg);
	auto result = data_ptr(new char[file_size]);
	file.read(result.get(), file_size);

	return result;
}
