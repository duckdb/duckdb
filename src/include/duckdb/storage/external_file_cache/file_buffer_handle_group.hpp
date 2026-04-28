//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/external_file_cache/file_buffer_handle_group.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/storage/buffer/buffer_handle.hpp"

namespace duckdb {

// A group of BufferHandles that together represent contiguous file content.
// For example, ZSTD supports stream-based decompression, which doesn't require to prepare a contiguous buffer;
// instead users are able to iterate all handles and decompress them on the fly.
class FileBufferHandleGroup {
public:
	// A single BufferHandle and its offset/length within the file.
	// In detail, the valid bytes of the handle are [start_offset, start_offset + length).
	struct MemoryHandle {
		BufferHandle handle;
		// Byte offset within handle's buffer where the relevant data begins
		idx_t start_offset;
		// Number of valid bytes starting from start offset
		idx_t length;
	};

	FileBufferHandleGroup() = default;
	explicit FileBufferHandleGroup(vector<MemoryHandle> handles_p);

	// Read-only access to the underlying handles.
	const vector<MemoryHandle> &GetHandles() const;

	// Util function to copy from the start of the group to the destination address for the requested number of bytes
	void CopyTo(data_ptr_t dest, idx_t nr_bytes) const;

	// Return a pointer to the start of the first handle in the group.
	// Warning: this function requires exactly one handle for zero-copy access, otherwise it will throw an exception.
	data_ptr_t Ptr() const;

private:
	// The list of MemoryHandles, could be empty.
	vector<MemoryHandle> handles;
};

} // namespace duckdb
