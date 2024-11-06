//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/csv_scanner/encode/csv_encoder.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/function/encoding_function.hpp"

namespace duckdb {

struct DBConfig;

//! Struct that holds encoder buffers
struct CSVEncoderBuffer {
	CSVEncoderBuffer() : encoded_buffer_size(0) {};
	void Initialize(idx_t encoded_buffer_size);

	char *Ptr() const;

	idx_t GetCapacity() const;

	idx_t GetSize() const;

	void SetSize(const idx_t buffer_size);

	bool HasDataToRead() const;

	void Reset();
	idx_t cur_pos = 0;
	//! The actual encoded buffer size, from the last file_handle read.
	idx_t actual_encoded_buffer_size = 0;

private:
	//! The encoded buffer, we only have one per file, so we cache it and make sure to pass over unused bytes.
	std::unique_ptr<char[]> encoded_buffer;
	//! The encoded buffer size is defined as buffer_size/GetRatio()
	idx_t encoded_buffer_size;
};

class CSVEncoder {
public:
	//! Constructor, basically takes an encoding and the output buffer size
	CSVEncoder(DBConfig &config, const string &encoding_name, idx_t buffer_size);
	//! Main encode function, it reads the file into an encoded buffer and converts it to the output buffer
	idx_t Encode(FileHandle &file_handle_input, char *output_buffer, const idx_t decoded_buffer_size);
	string encoding_name;

private:
	//! The actual encoded buffer
	CSVEncoderBuffer encoded_buffer;
	//! Potential remaining bytes
	CSVEncoderBuffer remaining_bytes_buffer;
	//! Actual Encoding Function
	optional_ptr<EncodingFunction> encoding_function;
};
} // namespace duckdb
