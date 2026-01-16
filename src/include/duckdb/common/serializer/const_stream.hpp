#pragma once

#include "duckdb/common/serializer/read_stream.hpp"
#include "duckdb/common/typedefs.hpp"

namespace duckdb {

class ConstReadStream : public ReadStream {
public:
	explicit ConstReadStream(const_data_ptr_t buffer, idx_t size) : buffer(buffer), size(size), position(0) {
	}

	void ReadData(data_ptr_t out_buffer, idx_t read_size) override {
		if (position + read_size > size) {
			throw SerializationException("Attempting to read past end of ConstReadStream");
		}
		memcpy(out_buffer, buffer + position, read_size);
		position += read_size;
	}

	void ReadData(QueryContext context, data_ptr_t out_buffer, idx_t read_size) override {
		ReadData(out_buffer, read_size);
	}

private:
	const_data_ptr_t buffer;
	idx_t size;
	idx_t position;
};

} // namespace duckdb
