//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/scan/csv/csv_buffer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"
#include "duckdb/execution/operator/scan/csv/csv_file_handle.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/block_manager.hpp"
#include "duckdb/storage/buffer/block_handle.hpp"

namespace duckdb {

class CSVBufferHandle {
public:
	CSVBufferHandle(BufferHandle handle_p, idx_t actual_size_p, const bool is_first_buffer_p,
	                const bool is_final_buffer_p, idx_t csv_global_state_p, idx_t start_position_p, idx_t file_idx_p)
	    : handle(std::move(handle_p)), actual_size(actual_size_p), is_first_buffer(is_first_buffer_p),
	      is_last_buffer(is_final_buffer_p), csv_global_start(csv_global_state_p), start_position(start_position_p),
	      file_idx(file_idx_p) {};
	CSVBufferHandle()
	    : actual_size(0), is_first_buffer(false), is_last_buffer(false), csv_global_start(0), start_position(0),
	      file_idx(0) {};
	//! Handle created during allocation
	BufferHandle handle;
	const idx_t actual_size;
	const bool is_first_buffer;
	const bool is_last_buffer;
	const idx_t csv_global_start;
	const idx_t start_position;
	const idx_t file_idx;
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
	          idx_t &global_csv_current_position, idx_t file_number);

	//! Constructor for `Next()` Buffers
	CSVBuffer(CSVFileHandle &file_handle, ClientContext &context, idx_t buffer_size, idx_t global_csv_current_position,
	          idx_t file_number_p);

	//! Creates a new buffer with the next part of the CSV File
	shared_ptr<CSVBuffer> Next(CSVFileHandle &file_handle, idx_t buffer_size, idx_t file_number);

	//! Gets the buffer actual size
	idx_t GetBufferSize();

	//! Gets the start position of the buffer, only relevant for the first time it's scanned
	idx_t GetStart();

	//! If this buffer is the last buffer of the CSV File
	bool IsCSVFileLastBuffer();

	//! Allocates internal buffer, sets 'block' and 'handle' variables.
	void AllocateBuffer(idx_t buffer_size);

	void Reload(CSVFileHandle &file_handle);
	//! Wrapper for the Pin Function, if it can seek, it means that the buffer might have been destroyed, hence we must
	//! Scan it from the disk file again.
	unique_ptr<CSVBufferHandle> Pin(CSVFileHandle &file_handle);
	//! Wrapper for the unpin
	void Unpin();
	char *Ptr() {
		return char_ptr_cast(handle.Ptr());
	}

	static constexpr idx_t CSV_BUFFER_SIZE = 32000000; // 32MB
	//! In case the file has a size < 32MB, we will use this size instead
	//! This is to avoid mallocing a lot of memory for a small file
	//! And if it's a compressed file we can't use the actual size of the file
	static constexpr idx_t CSV_MINIMUM_BUFFER_SIZE = 10000000; // 10MB
	//! If this is the last buffer of the CSV File
	bool last_buffer = false;

private:
	ClientContext &context;
	//! Actual size can be smaller than the buffer size in case we allocate it too optimistically.
	idx_t file_size;
	//! We need to check for Byte Order Mark, to define the start position of this buffer
	//! https://en.wikipedia.org/wiki/Byte_order_mark#UTF-8
	idx_t start_position = 0;
	//! If this is the first buffer of the CSV File
	bool first_buffer = false;
	//! Global position from the CSV File where this buffer starts
	idx_t global_csv_start = 0;
	//! Number of the file that is in this buffer
	idx_t file_number = 0;
	//! If we can seek in the file or not.
	//! If we can't seek, this means we can't destroy the buffers
	bool can_seek;
	//! -------- Allocated Block ---------//
	//! Block created in allocation
	shared_ptr<BlockHandle> block;
	BufferHandle handle;
};
} // namespace duckdb
