
#include "duckdb/common/buffered_file_handle.hpp"

namespace duckdb {

BufferedFileHandle::BufferedFileHandle(unique_ptr<FileHandle> inner_handle, size_t start, size_t end,
                                       Allocator &allocator)
    : WrappedFileHandle(std::move(inner_handle)), start(start), end(end) {
	if (start > end) {
		throw InvalidInputException("Range in BufferedFileHandle constructor wrongly set");
	}
	auto file_size = GetInner().GetFileSize();
	if (end > file_size) {
		end = file_size;
	}
	buffered = allocator.Allocate(end - start);
	GetInner().Read(buffered.get(), end - start, start);
}

BufferedFileHandle::~BufferedFileHandle() {
	// buffered is RAII managed
}

void BufferedFileHandle::Read(void *buffer, idx_t nr_bytes, idx_t location) {
	data_ptr_t ptr = static_cast<data_ptr_t>(buffer);
	idx_t current_location = location;

	if (current_location < start) {
		GetInner().Read(ptr, start - current_location, current_location);
		ptr += start - current_location;
		current_location = start;
	}
	if (current_location < end) {
		memcpy(ptr, buffered.get() + current_location, nr_bytes);
		ptr += nr_bytes;
		current_location += nr_bytes;
	}
	if (location + nr_bytes > end) {
		GetInner().Read(ptr, location + nr_bytes - current_location, current_location);
	}
}

} // namespace duckdb
