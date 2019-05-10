#include "common/buffered_serializer.hpp"

using namespace duckdb;
using namespace std;

BufferedSerializer::BufferedSerializer(uint64_t maximum_size)
    : BufferedSerializer(unique_ptr<uint8_t[]>(new uint8_t[maximum_size]), maximum_size) {
}

BufferedSerializer::BufferedSerializer(unique_ptr<uint8_t[]> data, uint64_t size)
    : maximum_size(size), data(data.get()) {
	blob.size = 0;
	blob.data = move(data);
}

void BufferedSerializer::Write(const uint8_t *buffer, uint64_t write_size) {
	if (blob.size + write_size >= maximum_size) {
		do {
			maximum_size *= 2;
		} while (blob.size + write_size > maximum_size);
		auto new_data = new uint8_t[maximum_size];
		memcpy(new_data, data, blob.size);
		data = new_data;
		blob.data = unique_ptr<uint8_t[]>(new_data);
	}

	memcpy(blob.data.get() + blob.size, buffer, write_size);
	blob.size += write_size;
}
