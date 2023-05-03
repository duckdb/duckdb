#include "duckdb/common/serializer/buffered_serializer.hpp"

#include <cstring>

namespace duckdb {

BufferedSerializer::BufferedSerializer(idx_t maximum_size)
    : BufferedSerializer(unique_ptr<data_t[]>(new data_t[maximum_size]), maximum_size) {
}

BufferedSerializer::BufferedSerializer(unique_ptr<data_t[]> data, idx_t size) : maximum_size(size), data(data.get()) {
	blob.size = 0;
	blob.data = std::move(data);
}

BufferedSerializer::BufferedSerializer(data_ptr_t data, idx_t size) : maximum_size(size), data(data) {
	blob.size = 0;
}

void BufferedSerializer::WriteData(const_data_ptr_t buffer, idx_t write_size) {
	if (blob.size + write_size >= maximum_size) {
		do {
			maximum_size *= 2;
		} while (blob.size + write_size > maximum_size);
		auto new_data = new data_t[maximum_size];
		memcpy(new_data, data, blob.size);
		data = new_data;
		blob.data = unique_ptr<data_t[]>(new_data);
	}

	memcpy(data + blob.size, buffer, write_size);
	blob.size += write_size;
}

} // namespace duckdb
