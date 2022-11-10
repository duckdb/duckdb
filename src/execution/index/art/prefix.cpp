#include "duckdb/execution/index/art/prefix.hpp"

namespace duckdb {

uint8_t *AllocateArray(idx_t size) {
	D_ASSERT(size > 0);
	return (uint8_t *)Allocator::DefaultAllocator().AllocateData(size * sizeof(uint8_t));
}

void DeleteArray(uint8_t *ptr, idx_t size) {
	D_ASSERT(size > 0);
	Allocator::DefaultAllocator().FreeData((data_ptr_t)ptr, size * sizeof(uint8_t));
}

uint32_t Prefix::Size() const {
	return size;
}

bool Prefix::IsInlined() const {
	return size <= PREFIX_INLINE_BYTES;
}

uint8_t *Prefix::GetPrefixData() {
	return IsInlined() ? &value.inlined[0] : value.ptr;
}

const uint8_t *Prefix::GetPrefixData() const {
	return IsInlined() ? &value.inlined[0] : value.ptr;
}

uint8_t *Prefix::AllocatePrefix(uint32_t size) {
	Destroy();

	this->size = size;
	uint8_t *prefix;
	if (IsInlined()) {
		prefix = &value.inlined[0];
	} else {
		// allocate new prefix
		value.ptr = AllocateArray(size);
		prefix = value.ptr;
	}
	return prefix;
}

Prefix::Prefix() : size(0) {
}

Prefix::Prefix(Key &key, uint32_t depth, uint32_t size) : size(0) {
	auto prefix = AllocatePrefix(size);

	// copy key to prefix
	idx_t prefix_idx = 0;
	for (idx_t i = depth; i < size + depth; i++) {
		prefix[prefix_idx++] = key.data[i];
	}
}

Prefix::Prefix(Prefix &other_prefix, uint32_t size) : size(0) {
	auto prefix = AllocatePrefix(size);

	// copy key to Prefix
	auto other_data = other_prefix.GetPrefixData();
	for (idx_t i = 0; i < size; i++) {
		prefix[i] = other_data[i];
	}
}

Prefix::~Prefix() {
	Destroy();
}

void Prefix::Destroy() {
	if (!IsInlined()) {
		DeleteArray(value.ptr, size);
		size = 0;
	}
}

uint8_t &Prefix::operator[](idx_t idx) {
	D_ASSERT(idx < Size());
	return GetPrefixData()[idx];
}

Prefix &Prefix::operator=(const Prefix &src) {
	auto prefix = AllocatePrefix(src.size);

	// copy prefix
	auto src_prefix = src.GetPrefixData();
	for (idx_t i = 0; i < src.size; i++) {
		prefix[i] = src_prefix[i];
	}
	size = src.size;
	return *this;
}

Prefix &Prefix::operator=(Prefix &&other) noexcept {
	std::swap(size, other.size);
	std::swap(value, other.value);
	return *this;
}

void Prefix::Overwrite(uint32_t new_size, uint8_t *data) {
	if (new_size <= PREFIX_INLINE_BYTES) {
		// new entry would be inlined
		// inline the data and destroy the pointer
		auto prefix = AllocatePrefix(new_size);
		for (idx_t i = 0; i < new_size; i++) {
			prefix[i] = data[i];
		}
		DeleteArray(data, new_size);
	} else {
		// new entry would not be inlined
		// take over the data directly
		Destroy();
		size = new_size;
		value.ptr = data;
	}
}

void Prefix::Concatenate(uint8_t key, Prefix &other) {
	auto new_length = size + 1 + other.size;
	// have to allocate space in our prefix array
	auto new_prefix = AllocateArray(new_length);
	idx_t new_prefix_idx = 0;
	// 1) add the to-be deleted node's prefix
	for (uint32_t i = 0; i < other.size; i++) {
		new_prefix[new_prefix_idx++] = other[i];
	}
	// 2) now move the current key as part of the prefix
	new_prefix[new_prefix_idx++] = key;
	// 3) move the existing prefix (if any)
	auto prefix = GetPrefixData();
	for (uint32_t i = 0; i < size; i++) {
		new_prefix[new_prefix_idx++] = prefix[i];
	}
	Overwrite(new_length, new_prefix);
}

uint8_t Prefix::Reduce(uint32_t n) {
	auto new_size = size - n - 1;
	auto prefix = GetPrefixData();
	auto key = prefix[n];
	if (new_size == 0) {
		Destroy();
		size = 0;
		return key;
	}
	auto new_prefix = AllocateArray(new_size);
	for (idx_t i = 0; i < new_size; i++) {
		new_prefix[i] = prefix[i + n + 1];
	}
	Overwrite(new_size, new_prefix);
	return key;
}

void Prefix::Serialize(duckdb::MetaBlockWriter &writer) {
	writer.Write(size);
	auto prefix = GetPrefixData();
	writer.WriteData(prefix, size);
}

void Prefix::Deserialize(duckdb::MetaBlockReader &reader) {
	auto prefix_size = reader.Read<uint32_t>();
	auto prefix = AllocatePrefix(prefix_size);
	this->size = prefix_size;
	reader.ReadData(prefix, size);
}

uint32_t Prefix::KeyMismatchPosition(Key &key, uint64_t depth) {
	uint64_t pos;
	auto prefix = GetPrefixData();
	for (pos = 0; pos < size; pos++) {
		if (key[depth + pos] != prefix[pos]) {
			return pos;
		}
	}
	return pos;
}

uint32_t Prefix::MismatchPosition(Prefix &other) {
	auto prefix = GetPrefixData();
	auto other_data = other.GetPrefixData();
	for (idx_t i = 0; i < size; i++) {
		if (prefix[i] != other_data[i]) {
			return i;
		}
	}
	return size;
}

} // namespace duckdb
