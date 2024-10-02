#include "duckdb/execution/index/art/art_key.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// ARTKey
//===--------------------------------------------------------------------===//

ARTKey::ARTKey() : len(0) {
}

ARTKey::ARTKey(const data_ptr_t data, idx_t len) : len(len), data(data) {
}

ARTKey::ARTKey(ArenaAllocator &allocator, idx_t len) : len(len) {
	data = allocator.Allocate(len);
}

template <>
ARTKey ARTKey::CreateARTKey(ArenaAllocator &allocator, string_t value) {
	auto string_data = const_data_ptr_cast(value.GetData());
	auto string_len = value.GetSize();

	// We escape \00 and \01.
	idx_t escape_count = 0;
	for (idx_t i = 0; i < string_len; i++) {
		if (string_data[i] <= 1) {
			escape_count++;
		}
	}

	idx_t len = string_len + escape_count + 1;
	auto data = allocator.Allocate(len);

	// Copy over the data and add escapes.
	idx_t pos = 0;
	for (idx_t i = 0; i < string_len; i++) {
		if (string_data[i] <= 1) {
			// Add escape.
			data[pos++] = '\01';
		}
		data[pos++] = string_data[i];
	}

	// End with a null-terminator.
	data[pos] = '\0';
	return ARTKey(data, len);
}

template <>
ARTKey ARTKey::CreateARTKey(ArenaAllocator &allocator, const char *value) {
	return ARTKey::CreateARTKey(allocator, string_t(value, UnsafeNumericCast<uint32_t>(strlen(value))));
}

template <>
void ARTKey::CreateARTKey(ArenaAllocator &allocator, ARTKey &key, string_t value) {
	key = ARTKey::CreateARTKey<string_t>(allocator, value);
}

template <>
void ARTKey::CreateARTKey(ArenaAllocator &allocator, ARTKey &key, const char *value) {
	ARTKey::CreateARTKey(allocator, key, string_t(value, UnsafeNumericCast<uint32_t>(strlen(value))));
}

ARTKey ARTKey::CreateKey(ArenaAllocator &allocator, PhysicalType type, Value &value) {
	D_ASSERT(type == value.type().InternalType());
	switch (type) {
	case PhysicalType::BOOL:
		return ARTKey::CreateARTKey<bool>(allocator, value);
	case PhysicalType::INT8:
		return ARTKey::CreateARTKey<int8_t>(allocator, value);
	case PhysicalType::INT16:
		return ARTKey::CreateARTKey<int16_t>(allocator, value);
	case PhysicalType::INT32:
		return ARTKey::CreateARTKey<int32_t>(allocator, value);
	case PhysicalType::INT64:
		return ARTKey::CreateARTKey<int64_t>(allocator, value);
	case PhysicalType::UINT8:
		return ARTKey::CreateARTKey<uint8_t>(allocator, value);
	case PhysicalType::UINT16:
		return ARTKey::CreateARTKey<uint16_t>(allocator, value);
	case PhysicalType::UINT32:
		return ARTKey::CreateARTKey<uint32_t>(allocator, value);
	case PhysicalType::UINT64:
		return ARTKey::CreateARTKey<uint64_t>(allocator, value);
	case PhysicalType::INT128:
		return ARTKey::CreateARTKey<hugeint_t>(allocator, value);
	case PhysicalType::UINT128:
		return ARTKey::CreateARTKey<uhugeint_t>(allocator, value);
	case PhysicalType::FLOAT:
		return ARTKey::CreateARTKey<float>(allocator, value);
	case PhysicalType::DOUBLE:
		return ARTKey::CreateARTKey<double>(allocator, value);
	case PhysicalType::VARCHAR:
		return ARTKey::CreateARTKey<string_t>(allocator, value);
	default:
		throw InternalException("Invalid type for the ART key.");
	}
}

bool ARTKey::operator>(const ARTKey &key) const {
	for (idx_t i = 0; i < MinValue(len, key.len); i++) {
		if (data[i] > key.data[i]) {
			return true;
		} else if (data[i] < key.data[i]) {
			return false;
		}
	}
	return len > key.len;
}

bool ARTKey::operator>=(const ARTKey &key) const {
	for (idx_t i = 0; i < MinValue(len, key.len); i++) {
		if (data[i] > key.data[i]) {
			return true;
		} else if (data[i] < key.data[i]) {
			return false;
		}
	}
	return len >= key.len;
}

bool ARTKey::operator==(const ARTKey &key) const {
	if (len != key.len) {
		return false;
	}
	for (idx_t i = 0; i < len; i++) {
		if (data[i] != key.data[i]) {
			return false;
		}
	}
	return true;
}

void ARTKey::Concat(ArenaAllocator &allocator, const ARTKey &other) {
	auto compound_data = allocator.Allocate(len + other.len);
	memcpy(compound_data, data, len);
	memcpy(compound_data + len, other.data, other.len);
	len += other.len;
	data = compound_data;
}

row_t ARTKey::GetRowId() const {
	D_ASSERT(len == sizeof(row_t));
	return Radix::DecodeData<row_t>(data);
}

idx_t ARTKey::GetMismatchPos(const ARTKey &other, const idx_t start) const {
	D_ASSERT(len <= other.len);
	D_ASSERT(start <= len);
	for (idx_t i = start; i < other.len; i++) {
		if (data[i] != other.data[i]) {
			return i;
		}
	}
	return DConstants::INVALID_INDEX;
}

//===--------------------------------------------------------------------===//
// ARTKeySection
//===--------------------------------------------------------------------===//

ARTKeySection::ARTKeySection(idx_t start, idx_t end, idx_t depth, data_t byte)
    : start(start), end(end), depth(depth), key_byte(byte) {
}

ARTKeySection::ARTKeySection(idx_t start, idx_t end, const unsafe_vector<ARTKey> &keys, const ARTKeySection &section)
    : start(start), end(end), depth(section.depth + 1), key_byte(keys[end].data[section.depth]) {
}

void ARTKeySection::GetChildSections(unsafe_vector<ARTKeySection> &sections, const unsafe_vector<ARTKey> &keys) {
	auto child_idx = start;
	for (idx_t i = start + 1; i <= end; i++) {
		if (keys[i - 1].data[depth] != keys[i].data[depth]) {
			sections.emplace_back(child_idx, i - 1, keys, *this);
			child_idx = i;
		}
	}
	sections.emplace_back(child_idx, end, keys, *this);
}

} // namespace duckdb
