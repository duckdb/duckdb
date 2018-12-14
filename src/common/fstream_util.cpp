#include "common/fstream_util.hpp"

using namespace std;
using namespace duckdb;

fstream FstreamUtil::OpenFile(const string &file_path, ios_base::openmode mode) {
	fstream new_file;
	new_file.open(file_path, mode);
	if (!new_file.good()) {
		throw IOException("Could not open File!" + file_path);
	}

	return new_file;
}

void FstreamUtil::CloseFile(fstream &file) {
	file.close();
	// check the success of the write
	if (file.fail()) {
		throw IOException("Failed to close the file!");
	}
}

size_t FstreamUtil::GetFileSize(fstream &file) {
	file.seekg(0, ios::end);
	return file.tellg();
}

data_ptr FstreamUtil::ReadBinary(fstream &file) {
	size_t file_size = GetFileSize(file);
	file.seekg(0, ios::beg);
	auto result = data_ptr(new char[file_size]);
	file.read(result.get(), file_size);

	return result;
}
