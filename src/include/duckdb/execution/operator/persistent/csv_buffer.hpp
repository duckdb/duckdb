//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/persistent/csv_buffer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"
#include "duckdb/execution/operator/persistent/csv_file_handle.hpp"
#include "duckdb/storage/buffer_manager.hpp"

namespace duckdb {

class CSVBuffer {
public:
	//! Colossal buffer size for multi-threading
	static constexpr idx_t INITIAL_BUFFER_SIZE_COLOSSAL = 32000000; // 32MB

	//! Constructor for Initial Buffer
	CSVBuffer(ClientContext &context, idx_t buffer_size_p, CSVFileHandle &file_handle, idx_t &global_csv_current_position);

	//! Constructor for `Next()` Buffers
	CSVBuffer(ClientContext &context, BufferHandle handle, idx_t buffer_size_p, idx_t actual_size_p, bool final_buffer, idx_t global_csv_current_position);

	//! Creates a new buffer with the next part of the CSV File
	unique_ptr<CSVBuffer> Next(CSVFileHandle &file_handle, idx_t buffer_size, idx_t &global_csv_current_position);

	//! Gets the buffer actual size
	idx_t GetBufferSize();

	//! Gets the start position of the buffer, only relevant for the first time it's scanned
	idx_t GetStart();

	//! If this buffer is the last buffer of the CSV File
	bool IsCSVFileLastBuffer();

	//! If this buffer is the first buffer of the CSV File
	bool IsCSVFileFirstBuffer();

	idx_t GetCSVGlobalStart();

	BufferHandle AllocateBuffer(idx_t buffer_size);

	char *Ptr() {
		return (char *)handle.Ptr();
	}

private:
	ClientContext &context;

	BufferHandle handle;
	//! Actual size can be smaller than the buffer size in case we allocate it too optimistically.
	idx_t actual_size;
	//! We need to check for Byte Order Mark, to define the start position of this buffer
	//! https://en.wikipedia.org/wiki/Byte_order_mark#UTF-8
	idx_t start_position = 0;
	//! If this is the last buffer of the CSV File
	bool last_buffer = false;
	//! If this is the first buffer of the CSV File
	bool first_buffer = false;
	//! Global position from the CSV File where this buffer starts
	idx_t global_csv_start = 0;
};
} // namespace duckdb
