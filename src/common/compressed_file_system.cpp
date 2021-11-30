#include "duckdb/common/compressed_file_system.hpp"

namespace duckdb {

StreamWrapper::~StreamWrapper() {
}

CompressedFile::CompressedFile(CompressedFileSystem &fs, unique_ptr<FileHandle> child_handle_p, const string &path)
	: FileHandle(fs, path), compressed_fs(fs), child_handle(move(child_handle_p)) {
}

CompressedFile::~CompressedFile() {
	Close();
}

void CompressedFile::Initialize() {
	Close();

	in_buf_size = compressed_fs.InBufferSize();
	out_buf_size = compressed_fs.OutBufferSize();
	in_buff = unique_ptr<data_t[]>(new data_t[in_buf_size]);
	in_buff_start = in_buff.get();
	in_buff_end = in_buff.get();
	out_buff = unique_ptr<data_t[]>(new data_t[out_buf_size]);
	out_buff_start = out_buff.get();
	out_buff_end = out_buff.get();

	stream_wrapper = compressed_fs.CreateStream();
	stream_wrapper->Initialize();
}

int64_t CompressedFile::ReadData(void *buffer, int64_t remaining) {
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
		if (!stream_wrapper) {
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
				stream_wrapper.reset();
				break;
			}
			in_buff_end = in_buff_start + sz;
		}

		auto finished = stream_wrapper->Read(in_buff_start, in_buff_end, out_buff_start, out_buff_end, out_buf_size);
		if (finished) {
			stream_wrapper.reset();
		}
	}
	return total_read;

}

void CompressedFile::Close() {
	stream_wrapper.reset();
	in_buff.reset();
	out_buff.reset();
}

int64_t CompressedFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes) {
	auto &compressed_file = (CompressedFile &)handle;
	return compressed_file.ReadData(buffer, nr_bytes);
}

void CompressedFileSystem::Reset(FileHandle &handle) {
	auto &compressed_file = (CompressedFile &)handle;
	compressed_file.child_handle->Reset();
	compressed_file.Initialize();
}

int64_t CompressedFileSystem::GetFileSize(FileHandle &handle) {
	auto &compressed_file = (CompressedFile &)handle;
	return compressed_file.child_handle->GetFileSize();
}

bool CompressedFileSystem::OnDiskFile(FileHandle &handle) {
	auto &compressed_file = (CompressedFile &)handle;
	return compressed_file.child_handle->OnDiskFile();
}

bool CompressedFileSystem::CanSeek() {
	return false;
}

}
