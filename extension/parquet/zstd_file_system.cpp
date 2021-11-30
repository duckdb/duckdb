#include "zstd_file_system.hpp"
#include "zstd.h"

namespace duckdb {

struct ZstdStreamWrapper {
	~ZstdStreamWrapper() {
		Close();
	}

	duckdb_zstd::ZSTD_DStream *zstd_stream_ptr = nullptr;

public:
	void Initialize() {
		Close();
		zstd_stream_ptr = duckdb_zstd::ZSTD_createDStream();
	}

	void Close() {
		if (!zstd_stream_ptr) {
			return;
		}
		duckdb_zstd::ZSTD_freeDStream(zstd_stream_ptr);
		zstd_stream_ptr = nullptr;
	}
};

class ZStdFile : public FileHandle {
public:
	ZStdFile(unique_ptr<FileHandle> child_handle_p, const string &path)
	    : FileHandle(zstd_fs, path), child_handle(move(child_handle_p)) {
		Initialize();
	}
	~ZStdFile() override {
		Close();
	}

	void Initialize();
	int64_t ReadData(void *buffer, int64_t nr_bytes);

	ZStdFileSystem zstd_fs;
	unique_ptr<FileHandle> child_handle;

protected:
	void Close() override {
		zstd_stream.reset();
		in_buff.reset();
		out_buff.reset();
	}

private:
	unique_ptr<ZstdStreamWrapper> zstd_stream;
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

void ZStdFile::Initialize() {
	Close();

	in_buf_size = duckdb_zstd::ZSTD_DStreamInSize();
	out_buf_size = duckdb_zstd::ZSTD_DStreamOutSize();
	in_buff = unique_ptr<data_t[]>(new data_t[in_buf_size]);
	in_buff_start = in_buff.get();
	in_buff_end = in_buff.get();
	out_buff = unique_ptr<data_t[]>(new data_t[out_buf_size]);
	out_buff_start = out_buff.get();
	out_buff_end = out_buff.get();

	zstd_stream = make_unique<ZstdStreamWrapper>();
	zstd_stream->Initialize();
}

unique_ptr<FileHandle> ZStdFileSystem::OpenCompressedFile(unique_ptr<FileHandle> handle) {
	auto path = handle->path;
	return make_unique<ZStdFile>(move(handle), path);
}

int64_t ZStdFile::ReadData(void *buffer, int64_t remaining) {
	auto &zstd_stream_ptr = zstd_stream->zstd_stream_ptr;
	idx_t total_read = 0;
	while (true) {
		// first check if there are input bytes available in the output buffers
		if (out_buff_start != out_buff_end) {
			// there is! copy it into the output buffer
			idx_t available = MinValue<idx_t>(remaining, out_buff_end - out_buff_start);
			memcpy(data_ptr_t(buffer) + total_read, out_buff_start, available);

			// increment the total read variables as required
			out_buff_start += available;
			total_read += available;
			remaining -= available;
			if (remaining == 0) {
				// done! read enough
				return total_read;
			}
		}
		if (!zstd_stream_ptr) {
			return total_read;
		}

		// ran out of buffer: read more data from the child stream
		out_buff_start = out_buff.get();
		out_buff_end = out_buff.get();
		D_ASSERT(in_buff_start <= in_buff_end);
		D_ASSERT(in_buff_end <= in_buff_start + in_buf_size);

		// read more input if none available
		if (in_buff_start == in_buff_end) {
			// empty input buffer: refill from the start
			in_buff_start = in_buff.get();
			in_buff_end = in_buff_start;
			auto sz = child_handle->Read(in_buff.get(), in_buf_size);
			if (sz <= 0) {
				break;
			}
			in_buff_end = in_buff_start + sz;
		}

		duckdb_zstd::ZSTD_inBuffer in_buffer;
		duckdb_zstd::ZSTD_outBuffer out_buffer;

		in_buffer.src = in_buff_start;
		in_buffer.size = in_buff_end - in_buff_start;
		in_buffer.pos = 0;

		out_buffer.dst = out_buff_start;
		out_buffer.size = out_buf_size;
		out_buffer.pos = 0;

		auto res = duckdb_zstd::ZSTD_decompressStream(zstd_stream_ptr, &out_buffer, &in_buffer);
		if (duckdb_zstd::ZSTD_isError(res)) {
			throw IOException(duckdb_zstd::ZSTD_getErrorName(res));
		}

		// update pointers following inflate()
		in_buff_start = (data_ptr_t)in_buffer.src + in_buffer.pos;
		in_buff_end = (data_ptr_t)in_buffer.src + in_buffer.size;
		out_buff_end = (data_ptr_t)out_buffer.dst + out_buffer.pos;
	}
	return total_read;
}

int64_t ZStdFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes) {
	auto &gzip_file = (ZStdFile &)handle;
	return gzip_file.ReadData(buffer, nr_bytes);
}

void ZStdFileSystem::Reset(FileHandle &handle) {
	auto &gzip_file = (ZStdFile &)handle;
	gzip_file.child_handle->Reset();
	gzip_file.Initialize();
}

int64_t ZStdFileSystem::GetFileSize(FileHandle &handle) {
	auto &gzip_file = (ZStdFile &)handle;
	return gzip_file.child_handle->GetFileSize();
}

bool ZStdFileSystem::OnDiskFile(FileHandle &handle) {
	auto &gzip_file = (ZStdFile &)handle;
	return gzip_file.child_handle->OnDiskFile();
}

} // namespace duckdb
