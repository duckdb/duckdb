#include "common/serializer.hpp"

using namespace duckdb;
using namespace std;

Serializer::Serializer(uint64_t maximum_size)
    : Serializer(unique_ptr<uint8_t[]>(new uint8_t[maximum_size]), maximum_size) {
}

Serializer::Serializer(unique_ptr<uint8_t[]> data, uint64_t size) : maximum_size(size), data(data.get()) {
	blob.size = 0;
	blob.data = move(data);
}

Serializer::Serializer(uint8_t *data) : maximum_size((uint64_t)-1), data(data) {
	blob.size = 0;
}

Deserializer::Deserializer(uint8_t *ptr, uint64_t data) : ptr(ptr), endptr(ptr + data) {
}

template <> string Deserializer::Read() {
	auto size = Read<uint32_t>();
	if (ptr + size > endptr) {
		throw SerializationException("Failed to deserialize object");
	}
	auto value = string((char *)ptr, size);
	ptr += size;
	return value;
}
