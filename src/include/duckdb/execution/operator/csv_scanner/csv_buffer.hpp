//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/csv_scanner/csv_buffer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"
#include "duckdb/execution/operator/csv_scanner/csv_file_handle.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/block_manager.hpp"
#include "duckdb/storage/buffer/block_handle.hpp"

namespace duckdb {

class CSVBufferHandle {
public:
	CSVBufferHandle(BufferHandle handle_p, idx_t actual_size_p, idx_t requested_size_p, const bool is_final_buffer_p,
	                idx_t buffer_index_p)
	    : handle(std::move(handle_p)), actual_size(actual_size_p), requested_size(requested_size_p),
	      is_last_buffer(is_final_buffer_p), buffer_idx(buffer_index_p) {};
	CSVBufferHandle() : actual_size(0), requested_size(0), is_last_buffer(false), buffer_idx(0) {};
	~CSVBufferHandle() {
	}
	//! Handle created during allocation
	BufferHandle handle;
	const idx_t actual_size;
	const idx_t requested_size;
	const bool is_last_buffer;
	const idx_t buffer_idx;
	inline char *Ptr() {
		return char_ptr_cast(handle.Ptr());
	}
};

//! CSV Buffers are parts of a decompressed CSV File.
//! For a decompressed file of 100Mb. With our Buffer size set to 32Mb, we would generate 4 buffers.
//! One for the first 32Mb, second and third for the other 32Mb, and the last one with 4 Mb
//! These buffers are actually used for sniffing and parsing!
class CSVBuffer {
public:
	//! Constructor for Initial Buffer
	CSVBuffer(ClientContext &context, idx_t buffer_size_p, CSVFileHandle &file_handle,
	          const idx_t &global_csv_current_position);

	//! Constructor for `Next()` Buffers
	CSVBuffer(CSVFileHandle &file_handle, ClientContext &context, idx_t buffer_size, idx_t global_csv_current_position,
	          idx_t buffer_idx);

	//! Creates a new buffer with the next part of the CSV File
	shared_ptr<CSVBuffer> Next(CSVFileHandle &file_handle, idx_t buffer_size, bool &has_seeked) const;

	//! Gets the buffer actual size
	idx_t GetBufferSize() const;

	//! If this buffer is the last buffer of the CSV File
	bool IsCSVFileLastBuffer() const;

	//! Allocates internal buffer, sets 'block' and 'handle' variables.
	void AllocateBuffer(idx_t buffer_size);

	void Reload(CSVFileHandle &file_handle);
	//! Wrapper for the Pin Function, if it can seek, it means that the buffer might have been destroyed, hence we must
	//! Scan it from the disk file again.
	shared_ptr<CSVBufferHandle> Pin(CSVFileHandle &file_handle, bool &has_seeked);
	//! Wrapper for unpin
	void Unpin();
	char *Ptr() {
		return char_ptr_cast(handle.Ptr());
	}
	bool IsUnloaded() const {
		return block->IsUnloaded();
	}

	//! By default, we use CSV_BUFFER_SIZE to allocate each buffer
	static constexpr idx_t ROWS_PER_BUFFER = 16;
	static constexpr idx_t MIN_ROWS_PER_BUFFER = 4;

	bool last_buffer = false;

private:
	ClientContext &context;
	//! Actual size can be smaller than the buffer size in case we allocate it too optimistically.
	idx_t actual_buffer_size;
	idx_t requested_size;
	//! Global position from the CSV File where this buffer starts
	idx_t global_csv_start = 0;
	//! If we can seek in the file or not.
	bool can_seek;
	//! If this file is being fed by a pipe.
	bool is_pipe;
	//! Buffer Index, used as a batch index for insertion-order preservation
	idx_t buffer_idx = 0;
	//! -------- Allocated Block ---------//
	//! Block created in allocation
	shared_ptr<BlockHandle> block;
	BufferHandle handle;
};
} // namespace duckdb
