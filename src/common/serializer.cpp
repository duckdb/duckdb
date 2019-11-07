#include "duckdb/common/serializer.hpp"

using namespace duckdb;
using namespace std;

template <> string Deserializer::Read() {
	uint32_t size = Read<uint32_t>();
	auto buffer = unique_ptr<data_t[]>(new data_t[size]);
	ReadData(buffer.get(), size);
	return string((char *)buffer.get(), size);
}
