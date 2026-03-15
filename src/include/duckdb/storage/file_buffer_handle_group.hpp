//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/file_buffer_handle_group.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/storage/buffer/buffer_handle.hpp"

namespace duckdb {

class FileBufferHandleGroup {
public:
	struct MemoryHandle {
		BufferHandle handle;
		idx_t start_offset;
		idx_t length;
	};

	vector<MemoryHandle> handles;

	// Copy from the start of the group to the destination pointer for the requested number of bytes
	void CopyTo(data_ptr_t dest, idx_t nr_bytes) const;
	// Return a pointer to the start of the first handle in the group.
	// Warning: this function requires exactly one handle for zero-copy access, otherwise it will throw an exception.
	data_ptr_t Ptr() const;
	// Return if the group contains exactly one handle.
	bool IsOneHandle() const;
};

} // namespace duckdb
