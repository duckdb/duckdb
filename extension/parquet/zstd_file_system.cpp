#include "zstd_file_system.hpp"

#include "zstd.h"

namespace duckdb {

struct ZstdStreamWrapper : public StreamWrapper {
	~ZstdStreamWrapper() override;

	CompressedFile *file = nullptr;
	duckdb_zstd::ZSTD_DStream *zstd_stream_ptr = nullptr;
	duckdb_zstd::ZSTD_CStream *zstd_compress_ptr = nullptr;
	bool writing = false;

public:
	void Initialize(CompressedFile &file, bool write) override;
	bool Read(StreamData &stream_data) override;
	void Write(CompressedFile &file, StreamData &stream_data, data_ptr_t buffer, int64_t nr_bytes) override;

	void Close() override;

	void FlushStream();
};

ZstdStreamWrapper::~ZstdStreamWrapper() {
	if (Exception::UncaughtException()) {
		return;
	}
	try {
		Close();
	} catch (...) { // NOLINT: swallow exceptions in destructor
	}
}

void ZstdStreamWrapper::Initialize(CompressedFile &file, bool write) {
	Close();
	this->file = &file;
	this->writing = write;
	if (write) {
		zstd_compress_ptr = duckdb_zstd::ZSTD_createCStream();
	} else {
		zstd_stream_ptr = duckdb_zstd::ZSTD_createDStream();
	}
}

bool ZstdStreamWrapper::Read(StreamData &sd) {
	D_ASSERT(!writing);

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

	sd.in_buff_start = (data_ptr_t)in_buffer.src + in_buffer.pos;  // NOLINT
	sd.in_buff_end = (data_ptr_t)in_buffer.src + in_buffer.size;   // NOLINT
	sd.out_buff_end = (data_ptr_t)out_buffer.dst + out_buffer.pos; // NOLINT
	return false;
}

void ZstdStreamWrapper::Write(CompressedFile &file, StreamData &sd, data_ptr_t uncompressed_data,
                              int64_t uncompressed_size) {
	D_ASSERT(writing);

	auto remaining = uncompressed_size;
	while (remaining > 0) {
		D_ASSERT(sd.out_buff.get() + sd.out_buf_size > sd.out_buff_start);
		idx_t output_remaining = (sd.out_buff.get() + sd.out_buf_size) - sd.out_buff_start;

		duckdb_zstd::ZSTD_inBuffer in_buffer;
		duckdb_zstd::ZSTD_outBuffer out_buffer;

		in_buffer.src = uncompressed_data;
		in_buffer.size = remaining;
		in_buffer.pos = 0;

		out_buffer.dst = sd.out_buff_start;
		out_buffer.size = output_remaining;
		out_buffer.pos = 0;
		auto res =
		    duckdb_zstd::ZSTD_compressStream2(zstd_compress_ptr, &out_buffer, &in_buffer, duckdb_zstd::ZSTD_e_continue);
		if (duckdb_zstd::ZSTD_isError(res)) {
			throw IOException(duckdb_zstd::ZSTD_getErrorName(res));
		}
		idx_t input_consumed = in_buffer.pos;
		idx_t written_to_output = out_buffer.pos;
		sd.out_buff_start += written_to_output;
		if (sd.out_buff_start == sd.out_buff.get() + sd.out_buf_size) {
			// no more output buffer available: flush
			file.child_handle->Write(sd.out_buff.get(), sd.out_buff_start - sd.out_buff.get());
			sd.out_buff_start = sd.out_buff.get();
		}
		uncompressed_data += input_consumed;
		remaining -= UnsafeNumericCast<int64_t>(input_consumed);
	}
}

void ZstdStreamWrapper::FlushStream() {
	auto &sd = file->stream_data;
	duckdb_zstd::ZSTD_inBuffer in_buffer;
	duckdb_zstd::ZSTD_outBuffer out_buffer;

	in_buffer.src = nullptr;
	in_buffer.size = 0;
	in_buffer.pos = 0;
	while (true) {
		idx_t output_remaining = (sd.out_buff.get() + sd.out_buf_size) - sd.out_buff_start;

		out_buffer.dst = sd.out_buff_start;
		out_buffer.size = output_remaining;
		out_buffer.pos = 0;

		auto res =
		    duckdb_zstd::ZSTD_compressStream2(zstd_compress_ptr, &out_buffer, &in_buffer, duckdb_zstd::ZSTD_e_end);
		if (duckdb_zstd::ZSTD_isError(res)) {
			throw IOException(duckdb_zstd::ZSTD_getErrorName(res));
		}
		idx_t written_to_output = out_buffer.pos;
		sd.out_buff_start += written_to_output;
		if (sd.out_buff_start > sd.out_buff.get()) {
			file->child_handle->Write(sd.out_buff.get(), sd.out_buff_start - sd.out_buff.get());
			sd.out_buff_start = sd.out_buff.get();
		}
		if (res == 0) {
			break;
		}
	}
}

void ZstdStreamWrapper::Close() {
	if (!zstd_stream_ptr && !zstd_compress_ptr) {
		return;
	}
	if (writing) {
		FlushStream();
	}
	if (zstd_stream_ptr) {
		duckdb_zstd::ZSTD_freeDStream(zstd_stream_ptr);
	}
	if (zstd_compress_ptr) {
		duckdb_zstd::ZSTD_freeCStream(zstd_compress_ptr);
	}
	zstd_stream_ptr = nullptr;
	zstd_compress_ptr = nullptr;
}

class ZStdFile : public CompressedFile {
public:
	ZStdFile(unique_ptr<FileHandle> child_handle_p, const string &path, bool write)
	    : CompressedFile(zstd_fs, std::move(child_handle_p), path) {
		Initialize(write);
	}

	FileCompressionType GetFileCompressionType() override {
		return FileCompressionType::ZSTD;
	}

	ZStdFileSystem zstd_fs;
};

unique_ptr<FileHandle> ZStdFileSystem::OpenCompressedFile(unique_ptr<FileHandle> handle, bool write) {
	auto path = handle->path;
	return make_uniq<ZStdFile>(std::move(handle), path, write);
}

unique_ptr<StreamWrapper> ZStdFileSystem::CreateStream() {
	return make_uniq<ZstdStreamWrapper>();
}

idx_t ZStdFileSystem::InBufferSize() {
	return duckdb_zstd::ZSTD_DStreamInSize();
}

idx_t ZStdFileSystem::OutBufferSize() {
	return duckdb_zstd::ZSTD_DStreamOutSize();
}

int64_t ZStdFileSystem::DefaultCompressionLevel() {
	return duckdb_zstd::ZSTD_defaultCLevel();
}

int64_t ZStdFileSystem::MinimumCompressionLevel() {
	return duckdb_zstd::ZSTD_minCLevel();
}

int64_t ZStdFileSystem::MaximumCompressionLevel() {
	return duckdb_zstd::ZSTD_maxCLevel();
}

} // namespace duckdb
