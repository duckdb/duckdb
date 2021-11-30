#include "zstd_file_system.hpp"
#include "zstd.h"

namespace duckdb {

struct ZstdStreamWrapper : public StreamWrapper {
	~ZstdStreamWrapper() override {
		Close();
	}

	duckdb_zstd::ZSTD_DStream *zstd_stream_ptr = nullptr;

public:
	void Initialize() override {
		Close();
		zstd_stream_ptr = duckdb_zstd::ZSTD_createDStream();
	}

	bool Read(data_ptr_t &in_buff_start, data_ptr_t &in_buff_end, data_ptr_t &out_buff_start, data_ptr_t &out_buff_end, idx_t out_buff_size) override;

	void Close() {
		if (!zstd_stream_ptr) {
			return;
		}
		duckdb_zstd::ZSTD_freeDStream(zstd_stream_ptr);
		zstd_stream_ptr = nullptr;
	}
};

bool ZstdStreamWrapper::Read(data_ptr_t &in_buff_start, data_ptr_t &in_buff_end, data_ptr_t &out_buff_start, data_ptr_t &out_buff_end, idx_t out_buff_size) {
	duckdb_zstd::ZSTD_inBuffer in_buffer;
	duckdb_zstd::ZSTD_outBuffer out_buffer;

	in_buffer.src = in_buff_start;
	in_buffer.size = in_buff_end - in_buff_start;
	in_buffer.pos = 0;

	out_buffer.dst = out_buff_start;
	out_buffer.size = out_buff_size;
	out_buffer.pos = 0;

	auto res = duckdb_zstd::ZSTD_decompressStream(zstd_stream_ptr, &out_buffer, &in_buffer);
	if (duckdb_zstd::ZSTD_isError(res)) {
		throw IOException(duckdb_zstd::ZSTD_getErrorName(res));
	}

	in_buff_start = (data_ptr_t)in_buffer.src + in_buffer.pos;
	in_buff_end = (data_ptr_t)in_buffer.src + in_buffer.size;
	out_buff_end = (data_ptr_t)out_buffer.dst + out_buffer.pos;
	return false;
}

class ZStdFile : public CompressedFile {
public:
	ZStdFile(unique_ptr<FileHandle> child_handle_p, const string &path) :
		CompressedFile(zstd_fs, move(child_handle_p), path) {
		Initialize();
	}

	ZStdFileSystem zstd_fs;
};

unique_ptr<FileHandle> ZStdFileSystem::OpenCompressedFile(unique_ptr<FileHandle> handle) {
	auto path = handle->path;
	return make_unique<ZStdFile>(move(handle), path);
}

unique_ptr<StreamWrapper> ZStdFileSystem::CreateStream() {
	return make_unique<ZstdStreamWrapper>();
}

idx_t ZStdFileSystem::InBufferSize() {
	return duckdb_zstd::ZSTD_DStreamInSize();
}

idx_t ZStdFileSystem::OutBufferSize() {
	return duckdb_zstd::ZSTD_DStreamOutSize();
}

} // namespace duckdb
