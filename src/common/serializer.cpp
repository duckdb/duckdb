#include "duckdb/common/serializer.hpp"

namespace duckdb {

template <>
string Deserializer::Read() {
	uint32_t size = Read<uint32_t>();
	if (size == 0) {
		return string();
	}
	auto buffer = make_unsafe_uniq_array<data_t>(size);
	ReadData(buffer.get(), size);
	return string(const_char_ptr_cast(buffer.get()), size);
}

void Deserializer::ReadStringVector(vector<string> &list) {
	uint32_t sz = Read<uint32_t>();
	list.resize(sz);
	for (idx_t i = 0; i < sz; i++) {
		list[i] = Read<string>();
	}
}

} // namespace duckdb
