#include "duckdb/common/pipe_file_system.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

class PipeFile : public FileHandle {
public:
	explicit PipeFile(QueryContext context_p, unique_ptr<FileHandle> child_handle_p)
	    : FileHandle(pipe_fs, child_handle_p->path, child_handle_p->GetFlags()),
	      child_handle(std::move(child_handle_p)), context(context_p) {
	}

	PipeFileSystem pipe_fs;
	unique_ptr<FileHandle> child_handle;

public:
	int64_t ReadChunk(void *buffer, int64_t nr_bytes);
	int64_t WriteChunk(void *buffer, int64_t nr_bytes);

	void Close() override {
	}

private:
	QueryContext context;
};

int64_t PipeFile::ReadChunk(void *buffer, int64_t nr_bytes) {
	return child_handle->Read(context, buffer, UnsafeNumericCast<idx_t>(nr_bytes));
}
int64_t PipeFile::WriteChunk(void *buffer, int64_t nr_bytes) {
	return child_handle->Write(buffer, UnsafeNumericCast<idx_t>(nr_bytes));
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

timestamp_t PipeFileSystem::GetLastModifiedTime(FileHandle &handle) {
	auto &child_handle = *handle.Cast<PipeFile>().child_handle;
	return child_handle.file_system.GetLastModifiedTime(child_handle);
}

void PipeFileSystem::FileSync(FileHandle &handle) {
}

unique_ptr<FileHandle> PipeFileSystem::OpenPipe(QueryContext context, unique_ptr<FileHandle> handle) {
	return make_uniq<PipeFile>(context, std::move(handle));
}

} // namespace duckdb
