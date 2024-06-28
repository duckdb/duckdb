#include "duckdb/common/types/string_heap.hpp"

#include "duckdb/common/types/string_type.hpp"
#include "duckdb/common/algorithm.hpp"
#include "duckdb/common/exception.hpp"
#include "utf8proc_wrapper.hpp"

#include <cstring>

namespace duckdb {

StringHeap::StringHeap(Allocator &allocator) : allocator(allocator) {
}

void StringHeap::Destroy() {
	allocator.Destroy();
}

void StringHeap::Move(StringHeap &other) {
	other.allocator.Move(allocator);
}

string_t StringHeap::AddString(const char *data, idx_t len) {
	D_ASSERT(Utf8Proc::Analyze(data, len) != UnicodeType::INVALID);
	return AddBlob(data, len);
}

string_t StringHeap::AddString(const char *data) {
	return AddString(data, strlen(data));
}

string_t StringHeap::AddString(const string &data) {
	return AddString(data.c_str(), data.size());
}

string_t StringHeap::AddString(const string_t &data) {
	return AddString(data.GetData(), data.GetSize());
}

string_t StringHeap::AddBlob(const char *data, idx_t len) {
	auto insert_string = EmptyString(len);
	auto insert_pos = insert_string.GetDataWriteable();
	memcpy(insert_pos, data, len);
	insert_string.Finalize();
	return insert_string;
}

string_t StringHeap::AddBlob(const string_t &data) {
	return AddBlob(data.GetData(), data.GetSize());
}

string_t StringHeap::EmptyString(idx_t len) {
	D_ASSERT(len > string_t::INLINE_LENGTH);
	if (len > string_t::MAX_STRING_SIZE) {
		throw OutOfRangeException("Cannot create a string of size: '%d', the maximum supported string size is: '%d'",
		                          len, string_t::MAX_STRING_SIZE);
	}
	auto insert_pos = const_char_ptr_cast(allocator.Allocate(len));
	return string_t(insert_pos, UnsafeNumericCast<uint32_t>(len));
}

idx_t StringHeap::SizeInBytes() const {
	return allocator.SizeInBytes();
}

idx_t StringHeap::AllocationSize() const {
	return allocator.AllocationSize();
}

} // namespace duckdb
