#include "duckdb/common/varint.hpp"
#include "duckdb/common/types/varint.hpp"

namespace duckdb {

varint_t::varint_t(ArenaAllocator &allocator, idx_t blob_size) : string_t(blob_size), allocator(&allocator) {
	if (blob_size > string_t::INLINE_BYTES) {
		// This is a big boy, gotta allocate it
		auto ptr = allocator.Allocate(blob_size);
		SetPointer(reinterpret_cast<char *>(ptr));
	}
}

// varint_t::varint_t(string_t input) {
// 	value = input.GetString();
// }

varint_t varint_t::operator*(const varint_t &rhs) const {
	return *this;
}
varint_t &varint_t::operator+=(const varint_t &rhs) {
	return *this;
}

varint_t &varint_t::operator+=(const string_t &rhs) {
	return *this;
}
} // namespace duckdb
