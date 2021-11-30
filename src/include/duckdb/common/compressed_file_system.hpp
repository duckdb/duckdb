//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/compressed_file_system.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/file_system.hpp"

namespace duckdb {

struct StreamWrapper {
	DUCKDB_API virtual ~StreamWrapper();

	DUCKDB_API virtual void Initialize() = 0;
	DUCKDB_API virtual bool Read(data_ptr_t &in_buff_start, data_ptr_t &in_buff_end, data_ptr_t &out_buff_start, data_ptr_t &out_buff_end, idx_t out_buff_size) = 0;
};

class CompressedFileSystem : public FileSystem {
public:
	DUCKDB_API int64_t Read(FileHandle &handle, void *buffer, int64_t nr_bytes) override;

	DUCKDB_API void Reset(FileHandle &handle) override;

	DUCKDB_API int64_t GetFileSize(FileHandle &handle) override;

	DUCKDB_API bool OnDiskFile(FileHandle &handle) override;
	DUCKDB_API bool CanSeek() override;

	DUCKDB_API virtual unique_ptr<StreamWrapper> CreateStream() = 0;
	DUCKDB_API virtual idx_t InBufferSize() = 0;
	DUCKDB_API virtual idx_t OutBufferSize() = 0;
};

class CompressedFile : public FileHandle {
public:
	DUCKDB_API CompressedFile(CompressedFileSystem &fs, unique_ptr<FileHandle> child_handle_p, const string &path);
	DUCKDB_API virtual ~CompressedFile() override;

	CompressedFileSystem &compressed_fs;
	unique_ptr<FileHandle> child_handle;

public:
	DUCKDB_API void Initialize();
	DUCKDB_API int64_t ReadData(void *buffer, int64_t nr_bytes);

protected:
	DUCKDB_API void Close() override;

private:
	unique_ptr<StreamWrapper> stream_wrapper;
	// various buffers & pointers
	unique_ptr<data_t[]> in_buff;
	unique_ptr<data_t[]> out_buff;
	data_ptr_t out_buff_start = nullptr;
	data_ptr_t out_buff_end = nullptr;
	data_ptr_t in_buff_start = nullptr;
	data_ptr_t in_buff_end = nullptr;

	idx_t in_buf_size = 0;
	idx_t out_buf_size = 0;
};

} // namespace duckdb
