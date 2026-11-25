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
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/encoding_function.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/main/client_context.hpp"

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
	//! If this is the last buffer
	bool last_buffer = false;

private:
	//! The encoded buffer, we only have one per file, so we cache it and make sure to pass over unused bytes.
	duckdb::unique_ptr<char[]> encoded_buffer;
	//! The encoded buffer size is defined as buffer_size/GetRatio()
	idx_t encoded_buffer_size;
};

class CSVEncoder {
public:
	//! Constructor, basically takes an encoding and the output buffer size
	CSVEncoder(ClientContext &context, const string &encoding_name, idx_t buffer_size);
	//! Main encode function, it reads the file into an encoded buffer and converts it to the output buffer
	idx_t Encode(FileHandle &file_handle_input, char *output_buffer, const idx_t decoded_buffer_size);
	//! Resets the encoder so the file can be read again from the beginning
	void Reset();
	//! The encoded bytes read from the file
	idx_t EncodedBytesRead() const {
		return encoded_bytes_read;
	}
	string encoding_name;

private:
	QueryContext context;

	//! The actual encoded buffer
	CSVEncoderBuffer encoded_buffer;
	//! Decoded bytes that did not fit in the output buffer yet
	string pending_decoded;
	idx_t pending_pos = 0;
	//! The streaming overload of the encoding's scalar function, executed on (encoded buffer, is_last)
	unique_ptr<Expression> decode_expression;
	unique_ptr<ExpressionExecutor> decode_executor;
	DataChunk decode_input;
	//! The maximum number of input bytes that make up one character
	idx_t minimum_input_window = 1;
	//! Pass-on Byte, used to check if we are done with the file, but must be appended to next buffer
	char pass_on_byte;
	bool has_pass_on_byte = false;
	idx_t encoded_bytes_read = 0;

private:
	//! Decodes the encoded buffer into the output buffer until either one is exhausted
	idx_t DecodeBuffer(char *output_buffer, idx_t output_pos, idx_t output_size);
	//! Runs the encoding's scalar function over the input, returns the number of bytes consumed
	idx_t Decode(const char *input, idx_t input_size, bool is_last, string &decoded);
};
} // namespace duckdb
