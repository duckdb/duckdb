#include "common/serializer.hpp"

using namespace duckdb;
using namespace std;

template <> string Deserializer::Read() {
	uint32_t size = Read<uint32_t>();
	uint8_t buffer[size];
	Read(buffer, size);
	return string((char *)buffer, size);
}
