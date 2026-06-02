//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/memory_mapped_file.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/file_open_flags.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/typedefs.hpp"

namespace duckdb {

struct MMapOptions {
	//! Size of the virtual mapping. Writable mappings sparsely extend the file up to this
	//! size at open time so the region never has to be remapped. Ignored for read-only.
	idx_t reserve_size = 0;
};

//! A fixed-size memory-mapped view of a file. The mapping pointer is stable for the
//! lifetime of the object, so callers may share it across threads without synchronization.
class MemoryMappedFile {
public:
	DUCKDB_API MemoryMappedFile(string path, FileOpenFlags flags, data_ptr_t data, idx_t size);
	MemoryMappedFile(const MemoryMappedFile &) = delete;
	DUCKDB_API virtual ~MemoryMappedFile();

	//! Bounds-checked pointer to [location, location + nr_bytes) within the mapping.
	DUCKDB_API const_data_ptr_t GetData(idx_t location, idx_t nr_bytes) const;
	//! Bounds-checked mutable pointer; throws if the mapping was opened read-only.
	DUCKDB_API data_ptr_t GetDataMutable(idx_t location, idx_t nr_bytes);

	DUCKDB_API virtual void Sync() = 0;
	//! Punch a hole at [offset, offset + length), releasing backing disk space. Returns
	//! false if the platform does not support hole punching.
	DUCKDB_API virtual bool Trim(idx_t offset, idx_t length) = 0;
	DUCKDB_API virtual bool OnDiskFile() const {
		return true;
	}
	DUCKDB_API virtual void Close() = 0;

	DUCKDB_API idx_t Size() const {
		return size;
	}
	const string &GetPath() const {
		return path;
	}
	FileOpenFlags GetFlags() const {
		return flags;
	}

public:
	string path;
	FileOpenFlags flags;

protected:
	data_ptr_t data = nullptr;
	idx_t size = 0;
};

} // namespace duckdb
