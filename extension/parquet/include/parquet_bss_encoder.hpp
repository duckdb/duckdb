//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parquet_bss_encoder.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "decode_utils.hpp"

namespace duckdb {

class BssEncoder {
public:
	explicit BssEncoder(const idx_t total_value_count_p, const idx_t bit_width_p)
	    : total_value_count(total_value_count_p), bit_width(bit_width_p), count(0),
	      buffer(Allocator::DefaultAllocator().Allocate(total_value_count * bit_width + 1)) {
	}

public:
	template <class T>
	void WriteValue(const T &value) {
		D_ASSERT(sizeof(T) == bit_width);
		for (idx_t i = 0; i < sizeof(T); i++) {
			buffer.get()[i * total_value_count + count] = reinterpret_cast<const_data_ptr_t>(&value)[i];
		}
		count++;
	}

	void FinishWrite(WriteStream &writer) {
		writer.WriteData(buffer.get(), total_value_count * bit_width);
	}

private:
	const idx_t total_value_count;
	const idx_t bit_width;

	idx_t count;
	AllocatedData buffer;
};

namespace bss_encoder {

template <class T>
void WriteValue(BssEncoder &encoder, const T &value) {
	throw InternalException("Can't write type to BYTE_STREAM_SPLIT column");
}

template <>
void WriteValue(BssEncoder &encoder, const float &value) {
	encoder.WriteValue(value);
}

template <>
void WriteValue(BssEncoder &encoder, const double &value) {
	encoder.WriteValue(value);
}

} // namespace bss_encoder

} // namespace duckdb
