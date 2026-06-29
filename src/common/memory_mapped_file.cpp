#include "duckdb/common/memory_mapped_file.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_system.hpp"

namespace duckdb {

// Platform-specific MemoryMappedFile subclasses and the LocalFileSystem::MemoryMapFile
// factory live in src/common/local_file_system.cpp.
MemoryMappedFile::MemoryMappedFile(string path_p, FileOpenFlags flags_p, data_ptr_t data_p, idx_t size_p)
    : path(std::move(path_p)), flags(flags_p), data(data_p), size(size_p) {
}

MemoryMappedFile::~MemoryMappedFile() {
}

const_data_ptr_t MemoryMappedFile::GetData(idx_t location, idx_t nr_bytes) const {
	if (location + nr_bytes > size) {
		throw IOException("MemoryMappedFile::GetData: out-of-bounds access in file \"%s\" "
		                  "(location=%llu, nr_bytes=%llu, size=%llu)",
		                  path, location, nr_bytes, size);
	}
	return data + location;
}

data_ptr_t MemoryMappedFile::GetDataMutable(idx_t location, idx_t nr_bytes) {
	if (location + nr_bytes > size) {
		throw IOException("MemoryMappedFile::GetDataMutable: out-of-bounds access in file \"%s\" "
		                  "(location=%llu, nr_bytes=%llu, size=%llu)",
		                  path, location, nr_bytes, size);
	}
	return data + location;
}

unique_ptr<MemoryMappedFile> FileSystem::MemoryMapFile(const OpenFileInfo &path, FileOpenFlags flags,
                                                       const MMapOptions &options, optional_ptr<FileOpener> opener) {
	throw NotImplementedException("Memory-mapped file access is not supported by this file system (%s) for path \"%s\"",
	                              GetName(), path.path);
}

} // namespace duckdb
