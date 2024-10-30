//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/csv_scanner/encode/csv_decoder.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/function/decoding_function.hpp"

namespace duckdb {

class DBConfig;

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

class CSVDecoder {
public:
	//! Constructor, basically takes an encoding and the output buffer size
	CSVDecoder(DBConfig &config, const string &enconding_name, idx_t buffer_size);
	//! Main decode function, it reads the file into an encoded buffer and converts it to the output buffer
	idx_t Decode(FileHandle &file_handle_input, char *output_buffer, const idx_t nr_bytes_to_read);
	string encoding_name;

private:
	//! The actual encoded buffer
	CSVEncoderBuffer encoded_buffer;
	//! Potential remaining bytes
	CSVEncoderBuffer remaining_bytes_buffer;
	//! Actual Decoding Function
	optional_ptr<DecodingFunction> decoding_function;
};
} // namespace duckdb
