//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parquet_bss_decoder.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once
#include "parquet_types.h"
#include "resizable_buffer.hpp"

namespace duckdb {

/// Decoder for the Byte Stream Split encoding
class BssDecoder {
public:
	/// Create a decoder object. buffer/buffer_len is the encoded data.
	BssDecoder(data_ptr_t buffer, uint32_t buffer_len) : buffer_(buffer, buffer_len), value_offset_(0) {
	}

public:
	template <typename T>
	void GetBatch(data_ptr_t values_target_ptr, uint32_t batch_size) {
		if (buffer_.len % sizeof(T) != 0) {
			std::stringstream error;
			error << "Data buffer size for the BYTE_STREAM_SPLIT encoding (" << buffer_.len
			      << ") should be a multiple of the type size (" << sizeof(T) << ")";
			throw std::runtime_error(error.str());
		}
		uint32_t num_buffer_values = buffer_.len / sizeof(T);

		buffer_.available((value_offset_ + batch_size) * sizeof(T));

		for (uint32_t byte_offset = 0; byte_offset < sizeof(T); ++byte_offset) {
			data_ptr_t input_bytes = buffer_.ptr + byte_offset * num_buffer_values + value_offset_;
			for (uint32_t i = 0; i < batch_size; ++i) {
				values_target_ptr[byte_offset + i * sizeof(T)] = *(input_bytes + i);
			}
		}
		value_offset_ += batch_size;
	}

	template <typename T>
	void Skip(uint32_t batch_size) {
		if (buffer_.len % sizeof(T) != 0) {
			std::stringstream error;
			error << "Data buffer size for the BYTE_STREAM_SPLIT encoding (" << buffer_.len
			      << ") should be a multiple of the type size (" << sizeof(T) << ")";
			throw std::runtime_error(error.str());
		}
		buffer_.available((value_offset_ + batch_size) * sizeof(T));
		value_offset_ += batch_size;
	}

private:
	ByteBuffer buffer_;
	uint32_t value_offset_;
};

} // namespace duckdb
