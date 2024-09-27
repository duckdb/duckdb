#include "duckdb/common/compressed_file_system.hpp"
#include "duckdb/common/numeric_utils.hpp"

namespace duckdb {

StreamWrapper::~StreamWrapper() {
}

CompressedFile::CompressedFile(CompressedFileSystem &fs, unique_ptr<FileHandle> child_handle_p, const string &path)
    : FileHandle(fs, path), compressed_fs(fs), child_handle(std::move(child_handle_p)) {
}

CompressedFile::~CompressedFile() {
	CompressedFile::Close();
}

void CompressedFile::Initialize(bool write) {
	Close();

	this->write = write;
	stream_data.in_buf_size = compressed_fs.InBufferSize();
	stream_data.out_buf_size = compressed_fs.OutBufferSize();
	stream_data.in_buff = make_unsafe_uniq_array<data_t>(stream_data.in_buf_size);
	stream_data.in_buff_start = stream_data.in_buff.get();
	stream_data.in_buff_end = stream_data.in_buff.get();
	stream_data.out_buff = make_unsafe_uniq_array<data_t>(stream_data.out_buf_size);
	stream_data.out_buff_start = stream_data.out_buff.get();
	stream_data.out_buff_end = stream_data.out_buff.get();

	stream_wrapper = compressed_fs.CreateStream();
	stream_wrapper->Initialize(*this, write);
}

idx_t CompressedFile::GetProgress() {
	return current_position;
}

int64_t CompressedFile::ReadData(void *buffer, int64_t remaining) {
	idx_t total_read = 0;
	while (true) {
		// first check if there are input bytes available in the output buffers
		if (stream_data.out_buff_start != stream_data.out_buff_end) {
			// there is! copy it into the output buffer
			auto available =
			    MinValue<idx_t>(UnsafeNumericCast<idx_t>(remaining),
			                    UnsafeNumericCast<idx_t>(stream_data.out_buff_end - stream_data.out_buff_start));
			memcpy(data_ptr_t(buffer) + total_read, stream_data.out_buff_start, available);

			// increment the total read variables as required
			stream_data.out_buff_start += available;
			total_read += available;
			remaining = UnsafeNumericCast<int64_t>(UnsafeNumericCast<idx_t>(remaining) - available);
			if (remaining == 0) {
				// done! read enough
				return UnsafeNumericCast<int64_t>(total_read);
			}
		}
		if (!stream_wrapper) {
			return UnsafeNumericCast<int64_t>(total_read);
		}
		current_position += static_cast<idx_t>(stream_data.in_buff_end - stream_data.in_buff_start);
		// ran out of buffer: read more data from the child stream
		stream_data.out_buff_start = stream_data.out_buff.get();
		stream_data.out_buff_end = stream_data.out_buff.get();
		D_ASSERT(stream_data.in_buff_start <= stream_data.in_buff_end);
		D_ASSERT(stream_data.in_buff_end <= stream_data.in_buff_start + stream_data.in_buf_size);

		// read more input when requested and still data in the input stream
		if (stream_data.refresh && (stream_data.in_buff_end == stream_data.in_buff.get() + stream_data.in_buf_size)) {
			auto bufrem = stream_data.in_buff_end - stream_data.in_buff_start;
			// buffer not empty, move remaining bytes to the beginning
			memmove(stream_data.in_buff.get(), stream_data.in_buff_start, UnsafeNumericCast<size_t>(bufrem));
			stream_data.in_buff_start = stream_data.in_buff.get();
			// refill the rest of input buffer
			auto sz = child_handle->Read(stream_data.in_buff_start + bufrem,
			                             stream_data.in_buf_size - UnsafeNumericCast<idx_t>(bufrem));
			stream_data.in_buff_end = stream_data.in_buff_start + bufrem + sz;
			if (sz <= 0) {
				stream_wrapper.reset();
				break;
			}
		}

		// read more input if none available
		if (stream_data.in_buff_start == stream_data.in_buff_end) {
			// empty input buffer: refill from the start
			stream_data.in_buff_start = stream_data.in_buff.get();
			stream_data.in_buff_end = stream_data.in_buff_start;
			auto sz = child_handle->Read(stream_data.in_buff.get(), stream_data.in_buf_size);
			if (sz <= 0) {
				stream_wrapper.reset();
				break;
			}
			stream_data.in_buff_end = stream_data.in_buff_start + sz;
		}

		auto finished = stream_wrapper->Read(stream_data);
		if (finished) {
			stream_wrapper.reset();
		}
	}
	return UnsafeNumericCast<int64_t>(total_read);
}

int64_t CompressedFile::WriteData(data_ptr_t buffer, int64_t nr_bytes) {
	stream_wrapper->Write(*this, stream_data, buffer, nr_bytes);
	return nr_bytes;
}

void CompressedFile::Close() {
	if (stream_wrapper) {
		stream_wrapper->Close();
		stream_wrapper.reset();
	}
	stream_data.in_buff.reset();
	stream_data.out_buff.reset();
	stream_data.out_buff_start = nullptr;
	stream_data.out_buff_end = nullptr;
	stream_data.in_buff_start = nullptr;
	stream_data.in_buff_end = nullptr;
	stream_data.in_buf_size = 0;
	stream_data.out_buf_size = 0;
	stream_data.refresh = false;
}

int64_t CompressedFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes) {
	auto &compressed_file = handle.Cast<CompressedFile>();
	return compressed_file.ReadData(buffer, nr_bytes);
}

int64_t CompressedFileSystem::Write(FileHandle &handle, void *buffer, int64_t nr_bytes) {
	auto &compressed_file = handle.Cast<CompressedFile>();
	return compressed_file.WriteData(data_ptr_cast(buffer), nr_bytes);
}

void CompressedFileSystem::Reset(FileHandle &handle) {
	auto &compressed_file = handle.Cast<CompressedFile>();
	compressed_file.child_handle->Reset();
	compressed_file.Initialize(compressed_file.write);
}

int64_t CompressedFileSystem::GetFileSize(FileHandle &handle) {
	auto &compressed_file = handle.Cast<CompressedFile>();
	return NumericCast<int64_t>(compressed_file.child_handle->GetFileSize());
}

bool CompressedFileSystem::OnDiskFile(FileHandle &handle) {
	auto &compressed_file = handle.Cast<CompressedFile>();
	return compressed_file.child_handle->OnDiskFile();
}

bool CompressedFileSystem::CanSeek() {
	return false;
}

} // namespace duckdb
