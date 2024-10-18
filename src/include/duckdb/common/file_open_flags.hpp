//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/file_open_flags.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/enums/file_compression_type.hpp"

namespace duckdb {

enum class FileLockType : uint8_t { NO_LOCK = 0, READ_LOCK = 1, WRITE_LOCK = 2 };

class FileOpenFlags {
public:
	static constexpr idx_t FILE_FLAGS_READ = idx_t(1 << 0);
	static constexpr idx_t FILE_FLAGS_WRITE = idx_t(1 << 1);
	static constexpr idx_t FILE_FLAGS_DIRECT_IO = idx_t(1 << 2);
	static constexpr idx_t FILE_FLAGS_FILE_CREATE = idx_t(1 << 3);
	static constexpr idx_t FILE_FLAGS_FILE_CREATE_NEW = idx_t(1 << 4);
	static constexpr idx_t FILE_FLAGS_APPEND = idx_t(1 << 5);
	static constexpr idx_t FILE_FLAGS_PRIVATE = idx_t(1 << 6);
	static constexpr idx_t FILE_FLAGS_NULL_IF_NOT_EXISTS = idx_t(1 << 7);
	static constexpr idx_t FILE_FLAGS_PARALLEL_ACCESS = idx_t(1 << 8);
	static constexpr idx_t FILE_FLAGS_EXCLUSIVE_CREATE = idx_t(1 << 9);
	static constexpr idx_t FILE_FLAGS_NULL_IF_EXISTS = idx_t(1 << 10);

public:
	FileOpenFlags() = default;
	constexpr FileOpenFlags(idx_t flags) : flags(flags) { // NOLINT: allow implicit conversion
	}
	constexpr FileOpenFlags(FileLockType lock) : lock(lock) { // NOLINT: allow implicit conversion
	}
	constexpr FileOpenFlags(FileCompressionType compression) // NOLINT: allow implicit conversion
	    : compression(compression) {
	}
	constexpr FileOpenFlags(idx_t flags, FileLockType lock, FileCompressionType compression)
	    : flags(flags), lock(lock), compression(compression) {
	}

	static constexpr FileLockType MergeLock(FileLockType a, FileLockType b) {
		return a == FileLockType::NO_LOCK ? b : a;
	}

	static constexpr FileCompressionType MergeCompression(FileCompressionType a, FileCompressionType b) {
		return a == FileCompressionType::UNCOMPRESSED ? b : a;
	}

	inline constexpr FileOpenFlags operator|(FileOpenFlags b) const {
		return FileOpenFlags(flags | b.flags, MergeLock(lock, b.lock), MergeCompression(compression, b.compression));
	}
	inline FileOpenFlags &operator|=(FileOpenFlags b) {
		flags |= b.flags;
		lock = MergeLock(lock, b.lock);
		compression = MergeCompression(compression, b.compression);
		return *this;
	}

	FileLockType Lock() {
		return lock;
	}

	FileCompressionType Compression() {
		return compression;
	}

	void SetCompression(FileCompressionType new_compression) {
		compression = new_compression;
	}

	void Verify();

	inline bool OpenForReading() const {
		return flags & FILE_FLAGS_READ;
	}
	inline bool OpenForWriting() const {
		return flags & FILE_FLAGS_WRITE;
	}
	inline bool DirectIO() const {
		return flags & FILE_FLAGS_DIRECT_IO;
	}
	inline bool CreateFileIfNotExists() const {
		return flags & FILE_FLAGS_FILE_CREATE;
	}
	inline bool OverwriteExistingFile() const {
		return flags & FILE_FLAGS_FILE_CREATE_NEW;
	}
	inline bool OpenForAppending() const {
		return flags & FILE_FLAGS_APPEND;
	}
	inline bool CreatePrivateFile() const {
		return flags & FILE_FLAGS_PRIVATE;
	}
	inline bool ReturnNullIfNotExists() const {
		return flags & FILE_FLAGS_NULL_IF_NOT_EXISTS;
	}
	inline bool RequireParallelAccess() const {
		return flags & FILE_FLAGS_PARALLEL_ACCESS;
	}
	inline bool ExclusiveCreate() const {
		return flags & FILE_FLAGS_EXCLUSIVE_CREATE;
	}
	inline bool ReturnNullIfExists() const {
		return flags & FILE_FLAGS_NULL_IF_EXISTS;
	}
	inline idx_t GetFlagsInternal() const {
		return flags;
	}

private:
	idx_t flags = 0;
	FileLockType lock = FileLockType::NO_LOCK;
	FileCompressionType compression = FileCompressionType::UNCOMPRESSED;
};

class FileFlags {
public:
	//! Open file with read access
	static constexpr FileOpenFlags FILE_FLAGS_READ = FileOpenFlags(FileOpenFlags::FILE_FLAGS_READ);
	//! Open file with write access
	static constexpr FileOpenFlags FILE_FLAGS_WRITE = FileOpenFlags(FileOpenFlags::FILE_FLAGS_WRITE);
	//! Use direct IO when reading/writing to the file
	static constexpr FileOpenFlags FILE_FLAGS_DIRECT_IO = FileOpenFlags(FileOpenFlags::FILE_FLAGS_DIRECT_IO);
	//! Create file if not exists, can only be used together with WRITE
	static constexpr FileOpenFlags FILE_FLAGS_FILE_CREATE = FileOpenFlags(FileOpenFlags::FILE_FLAGS_FILE_CREATE);
	//! Always create a new file. If a file exists, the file is truncated. Cannot be used together with CREATE.
	static constexpr FileOpenFlags FILE_FLAGS_FILE_CREATE_NEW =
	    FileOpenFlags(FileOpenFlags::FILE_FLAGS_FILE_CREATE_NEW);
	//! Open file in append mode
	static constexpr FileOpenFlags FILE_FLAGS_APPEND = FileOpenFlags(FileOpenFlags::FILE_FLAGS_APPEND);
	//! Open file with restrictive permissions (600 on linux/mac) can only be used when creating, throws if file exists
	static constexpr FileOpenFlags FILE_FLAGS_PRIVATE = FileOpenFlags(FileOpenFlags::FILE_FLAGS_PRIVATE);
	//! Return NULL if the file does not exist instead of throwing an error
	static constexpr FileOpenFlags FILE_FLAGS_NULL_IF_NOT_EXISTS =
	    FileOpenFlags(FileOpenFlags::FILE_FLAGS_NULL_IF_NOT_EXISTS);
	//! Multiple threads may perform reads and writes in parallel
	static constexpr FileOpenFlags FILE_FLAGS_PARALLEL_ACCESS =
	    FileOpenFlags(FileOpenFlags::FILE_FLAGS_PARALLEL_ACCESS);
	//! Ensure that this call creates the file, throw is file exists
	static constexpr FileOpenFlags FILE_FLAGS_EXCLUSIVE_CREATE =
	    FileOpenFlags(FileOpenFlags::FILE_FLAGS_EXCLUSIVE_CREATE);
	//!  Return NULL if the file exist instead of throwing an error
	static constexpr FileOpenFlags FILE_FLAGS_NULL_IF_EXISTS = FileOpenFlags(FileOpenFlags::FILE_FLAGS_NULL_IF_EXISTS);
};

} // namespace duckdb
