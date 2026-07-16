#include "duckdb/storage/magic_bytes.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/prefetched_file_data.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/storage/storage_info.hpp"

#include <cstring>

namespace duckdb {

static constexpr idx_t MAGIC_BYTES_READ_SIZE = 16;
//! MainHeader + 2 DatabaseHeaders; prefetched so a later storage open serves the header reads from memory.
static constexpr idx_t HEADER_PREFETCH_SIZE = 3 * Storage::FILE_HEADER_SIZE;

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
                                         optional_ptr<PrefetchedFileData> out_prefetch) {
	if (path.empty() || path == IN_MEMORY_PATH) {
		return DataFileType::DUCKDB_FILE;
	}

	// The handle only reads the header and is closed on return; the database handle is opened later. On prefetch
	// we read the whole header region at once so the later open can serve the header reads from these bytes.
	auto handle = fs.OpenFile(path, FileFlags::FILE_FLAGS_READ | FileFlags::FILE_FLAGS_NULL_IF_NOT_EXISTS);
	if (!handle) {
		return DataFileType::FILE_DOES_NOT_EXIST;
	}

	// Zero-initialized so a short read at EOF still yields a well-defined (non-matching) classification.
	const idx_t read_size = out_prefetch ? HEADER_PREFETCH_SIZE : MAGIC_BYTES_READ_SIZE;
	auto buffer = make_unsafe_uniq_array<char>(read_size);
	memset(buffer.get(), 0, read_size);
	const idx_t to_read = MinValue<idx_t>(read_size, handle->GetFileSize());
	if (to_read > 0) {
		handle->Read(context, buffer.get(), to_read, 0);
	}

	auto file_type = ClassifyMagicBytes(buffer.get());

	// Hand the prefetched header bytes to the caller for DuckDB files. Only the `to_read` valid bytes are kept.
	if (out_prefetch && file_type == DataFileType::DUCKDB_FILE) {
		out_prefetch->header = make_shared_ptr<const string>(buffer.get(), to_read);
	}
	return file_type;
}

} // namespace duckdb
