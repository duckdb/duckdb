#include "duckdb/common/serializer/buffered_deserializer.hpp"

#include <cstring>

using namespace duckdb;
using namespace std;

BufferedDeserializer::BufferedDeserializer(data_ptr_t ptr, idx_t data_size) : ptr(ptr), endptr(ptr + data_size) {
}

BufferedDeserializer::BufferedDeserializer(BufferedSerializer &serializer)
    : BufferedDeserializer(serializer.data, serializer.maximum_size) {
}

void BufferedDeserializer::ReadData(data_ptr_t buffer, idx_t read_size) {
	if (ptr + read_size > endptr) {
		throw SerializationException("Failed to deserialize: not enough data in buffer to fulfill read request");
	}
	memcpy(buffer, ptr, read_size);
	ptr += read_size;
}
