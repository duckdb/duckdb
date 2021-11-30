#include "zstd_file_system.hpp"
#include "zstd.h"

namespace duckdb {

struct ZstdStreamWrapper : public StreamWrapper {
	~ZstdStreamWrapper() override {
		Close();
	}

	duckdb_zstd::ZSTD_DStream *zstd_stream_ptr = nullptr;

public:
	void Initialize(CompressedFile &file) override;
	bool Read(StreamData &stream_data) override;

	void Close();
};

void ZstdStreamWrapper::Initialize(CompressedFile &file) {
	Close();
	zstd_stream_ptr = duckdb_zstd::ZSTD_createDStream();
}

bool ZstdStreamWrapper::Read(StreamData &sd) {
	duckdb_zstd::ZSTD_inBuffer in_buffer;
	duckdb_zstd::ZSTD_outBuffer out_buffer;

	in_buffer.src = sd.in_buff_start;
	in_buffer.size = sd.in_buff_end - sd.in_buff_start;
	in_buffer.pos = 0;

	out_buffer.dst = sd.out_buff_start;
	out_buffer.size = sd.out_buf_size;
	out_buffer.pos = 0;

	auto res = duckdb_zstd::ZSTD_decompressStream(zstd_stream_ptr, &out_buffer, &in_buffer);
	if (duckdb_zstd::ZSTD_isError(res)) {
		throw IOException(duckdb_zstd::ZSTD_getErrorName(res));
	}

	sd.in_buff_start = (data_ptr_t)in_buffer.src + in_buffer.pos;
	sd.in_buff_end = (data_ptr_t)in_buffer.src + in_buffer.size;
	sd.out_buff_end = (data_ptr_t)out_buffer.dst + out_buffer.pos;
	return false;
}

void ZstdStreamWrapper::Close() {
	if (!zstd_stream_ptr) {
		return;
	}
	duckdb_zstd::ZSTD_freeDStream(zstd_stream_ptr);
	zstd_stream_ptr = nullptr;
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
