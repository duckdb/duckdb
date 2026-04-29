#include "duckdb/storage/magic_bytes.hpp"
#include "duckdb/common/file_system/buffered_file_handle.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/storage/storage_info.hpp"

namespace duckdb {

//! Main header + 2 DB headers (3 × 4 KiB). Prefetched so a later storage open
//! can reuse the buffered prefix instead of re-reading.
static constexpr const idx_t HEADER_PREFETCH_SIZE = 12288;
static constexpr const idx_t MAGIC_BYTES_READ_SIZE = 16;

static DataFileType ClassifyMagicBytes(const char *buffer) {
	if (memcmp(buffer, "SQLite format 3\0", 16) == 0) {
		return DataFileType::SQLITE_FILE;
	}
	if (memcmp(buffer, "PAR1", 4) == 0) {
		return DataFileType::PARQUET_FILE;
	}
	if (memcmp(buffer + MainHeader::MAGIC_BYTE_OFFSET, MainHeader::MAGIC_BYTES, MainHeader::MAGIC_BYTE_SIZE) == 0) {
		return DataFileType::DUCKDB_FILE;
	}
	return DataFileType::UNKNOWN_FILE;
}

DataFileType MagicBytes::CheckMagicBytes(QueryContext context, FileSystem &fs, const string &path,
                                         unique_ptr<BufferedFileHandle> *out_handle) {
	if (path.empty() || path == IN_MEMORY_PATH) {
		return DataFileType::DUCKDB_FILE;
	}

	// Reuse caller's handle if any, otherwise open into either out_handle or a local.
	unique_ptr<BufferedFileHandle> local_handle;
	auto &handle_slot = out_handle ? *out_handle : local_handle;
	if (!handle_slot) {
		auto raw_handle = fs.OpenFile(path, FileFlags::FILE_FLAGS_READ | FileFlags::FILE_FLAGS_NULL_IF_NOT_EXISTS);
		if (!raw_handle) {
			return DataFileType::FILE_DOES_NOT_EXIST;
		}
		handle_slot = make_uniq<BufferedFileHandle>(std::move(raw_handle));
	}

	handle_slot->RegisterPrefetch(0, HEADER_PREFETCH_SIZE);

	char buffer[MAGIC_BYTES_READ_SIZE] = {};
	handle_slot->Read(buffer, MAGIC_BYTES_READ_SIZE, 0);
	return ClassifyMagicBytes(buffer);
}

} // namespace duckdb
