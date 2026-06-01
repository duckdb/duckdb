#include "duckdb/storage/database_handle.hpp"

namespace duckdb {

DatabaseHandle::DatabaseHandle(unique_ptr<FileHandle> handle_p) : handle(std::move(handle_p)) {
}
DatabaseHandle::DatabaseHandle(unique_ptr<MemoryMappedFile> mmap_handle_p) : mmap_handle(std::move(mmap_handle_p)) {
}

bool DatabaseHandle::OnDiskFile() const {
	if (mmap_handle) {
		return mmap_handle->OnDiskFile();
	}
	return handle->OnDiskFile();
}

void DatabaseHandle::CheckMagicBytes(QueryContext context) {
	if (mmap_handle) {
		MainHeader::CheckMagicBytes(*mmap_handle);
	} else {
		MainHeader::CheckMagicBytes(context, *handle);
	}
}

void DatabaseHandle::Read(QueryContext context, FileBuffer &block, uint64_t location) const {
	if (mmap_handle) {
		block.Read(context, *mmap_handle, location);
	} else {
		block.Read(context, *handle, location);
	}
}

void DatabaseHandle::Write(QueryContext context, FileBuffer &buffer, uint64_t location) {
	if (mmap_handle) {
		EnsureMappedSize(location + buffer.AllocSize());
		buffer.Write(context, *mmap_handle, location);
	} else {
		buffer.Write(context, *handle, location);
	}
}

void DatabaseHandle::Sync() {
	if (mmap_handle) {
		mmap_handle->Sync();
	} else {
		handle->Sync();
	}
}

void DatabaseHandle::Truncate(idx_t new_size) {
	if (mmap_handle) {
		// File stays at reserve size in MAP mode; punch a hole to release the tail.
		mmap_handle->Trim(new_size, mmap_handle->Size() - new_size);
	} else {
		handle->Truncate(NumericCast<int64_t>(new_size));
	}
}

void DatabaseHandle::Trim(idx_t offset, idx_t length) {
	if (mmap_handle) {
		mmap_handle->Trim(offset, length);
	} else {
		handle->Trim(offset, length);
	}
}

FileHandle &DatabaseHandle::GetFileHandle() {
	return *handle;
}

void DatabaseHandle::EnsureMappedSize(idx_t required_size) const {
	if (!mmap_handle) {
		return;
	}
	if (required_size > mmap_handle->Size()) {
		throw IOException("Database file \"%s\" would grow to %llu bytes, which exceeds the memory-mapped reserved "
		                  "size of %llu bytes. Re-attach with a larger MMAP_RESERVE_SIZE, or without "
		                  "IO_MODE='MMAP', to allow further growth.",
		                  mmap_handle->GetPath(), required_size, mmap_handle->Size());
	}
}

} // namespace duckdb
