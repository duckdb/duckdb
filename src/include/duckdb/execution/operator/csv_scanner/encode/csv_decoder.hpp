//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/csv_scanner/encode/csv_decoder.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/typedefs.hpp"
#include "duckdb/execution/operator/csv_scanner/encode/csv_encoding.hpp"
#include "duckdb/common/file_system.hpp"

namespace duckdb {

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

private:
	//! The encoded buffer, we only have one per file, so we cache it and make sure to pass over unused bytes.
	std::unique_ptr<char[]> encoded_buffer;
	//! The actual encoded buffer size, from the last file_handle read.
	idx_t actual_encoded_buffer_size = 0;
	//! The encoded buffer size is defined as buffer_size/GetRatio()
	idx_t encoded_buffer_size;
};

class CSVDecoder {
public:
	//! Constructor, basically takes an encoding and the output buffer size
	CSVDecoder(CSVEncoding encoding, idx_t buffer_size);
	//! If this decoder is already on the UTF-8 format
	bool IsUTF8() const;
	//! Main decode function, it reads the file into an encoded buffer and converts it to the output buffer
	idx_t Decode(FileHandle &file_handle_input, char *output_buffer, const idx_t nr_bytes_to_read);

private:
	//! The encoding being use
	CSVEncoding encoding;
	//! The actual encoded buffer
	CSVEncoderBuffer encoded_buffer;
	//! Potential remaining bytes
	CSVEncoderBuffer remaining_bytes_buffer;
	//! What is the max buffer ratio (i.e., what's the worst case of this encoding to UTF-8)
	idx_t GetRatio() const;
	//! What's the max bytes one decode iteration can yield
	idx_t MaxDecodedBytesPerIteration() const;
	//! Decoding Switch-aroo
	void DecodeInternal(char *decoded_buffer, idx_t &decoded_buffer_start, const idx_t decoded_buffer_size);

	//! -------------------------------------------------------------------------------------------------------!//
	//! -------------------------------------------------------------------------------------------------------!//
	//! Actual decoding functions
	//! -------------------------------------------------------------------------------------------------------!//
	//! -------------------------------------------------------------------------------------------------------!//
	//! UTF16 -> UTF8
	void DecodeUTF16(char *decoded_buffer, idx_t &decoded_buffer_start, const idx_t decoded_buffer_size);
	//! LATIN1 -> UTF8
	void DecodeLatin1(char *decoded_buffer, idx_t &decoded_buffer_start, const idx_t decoded_buffer_size);
};
} // namespace duckdb
