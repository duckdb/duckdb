#include "duckdb/common/pipe_file_system.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/helper.hpp"

namespace duckdb {
class PipeFile : public FileHandle {
public:
	PipeFile(unique_ptr<FileHandle> child_handle_p, const string &path)
	    : FileHandle(pipe_fs, path), child_handle(std::move(child_handle_p)) {
	}

	PipeFileSystem pipe_fs;
	unique_ptr<FileHandle> child_handle;

public:
	int64_t ReadChunk(void *buffer, int64_t nr_bytes);
	int64_t WriteChunk(void *buffer, int64_t nr_bytes);

	void Close() override {
	}
};

int64_t PipeFile::ReadChunk(void *buffer, int64_t nr_bytes) {
	return child_handle->Read(buffer, nr_bytes);
}
int64_t PipeFile::WriteChunk(void *buffer, int64_t nr_bytes) {
	return child_handle->Write(buffer, nr_bytes);
}

void PipeFileSystem::Reset(FileHandle &handle) {
	throw InternalException("Cannot reset pipe file system");
}

int64_t PipeFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes) {
	auto &pipe = handle.Cast<PipeFile>();
	return pipe.ReadChunk(buffer, nr_bytes);
}

int64_t PipeFileSystem::Write(FileHandle &handle, void *buffer, int64_t nr_bytes) {
	auto &pipe = handle.Cast<PipeFile>();
	return pipe.WriteChunk(buffer, nr_bytes);
}

int64_t PipeFileSystem::GetFileSize(FileHandle &handle) {
	return 0;
}

void PipeFileSystem::FileSync(FileHandle &handle) {
}

unique_ptr<FileHandle> PipeFileSystem::OpenPipe(unique_ptr<FileHandle> handle) {
	auto path = handle->path;
	return make_uniq<PipeFile>(std::move(handle), path);
}

} // namespace duckdb
