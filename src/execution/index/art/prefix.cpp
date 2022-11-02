#include "duckdb/execution/index/art/prefix.hpp"

namespace duckdb {

uint32_t Prefix::Size() const {
	return size;
}

Prefix::Prefix() : size(0) {
}

Prefix::Prefix(Key &key, uint32_t depth, uint32_t size) : size(size) {
	// allocate new prefix
	prefix = unique_ptr<uint8_t[]>(new uint8_t[size]);

	// copy key to prefix
	idx_t prefix_idx = 0;
	for (idx_t i = depth; i < size + depth; i++) {
		prefix[prefix_idx++] = key.data[i];
	}
}

Prefix::Prefix(Prefix &other_prefix, uint32_t size) : size(size) {
	// allocate new prefix
	prefix = unique_ptr<uint8_t[]>(new uint8_t[size]);

	// copy key to Prefix
	for (idx_t i = 0; i < size; i++) {
		prefix[i] = other_prefix[i];
	}
}

uint8_t &Prefix::operator[](idx_t idx) {
	D_ASSERT(idx < Size());
	return prefix[idx];
}

Prefix &Prefix::operator=(const Prefix &src) {
	// allocate new prefix
	prefix = unique_ptr<uint8_t[]>(new uint8_t[src.size]);

	// copy prefix
	for (idx_t i = 0; i < src.size; i++) {
		prefix[i] = src.prefix[i];
	}
	size = src.size;
	return *this;
}

Prefix &Prefix::operator=(Prefix &&other) noexcept {
	prefix = move(other.prefix);
	size = other.size;
	return *this;
}

void Prefix::Concatenate(uint8_t key, Prefix &other) {
	auto new_length = size + 1 + other.size;
	// have to allocate space in our prefix array
	unique_ptr<uint8_t[]> new_prefix = unique_ptr<uint8_t[]>(new uint8_t[new_length]);
	idx_t new_prefix_idx = 0;
	// 1) add the to-be deleted node's prefix
	for (uint32_t i = 0; i < other.size; i++) {
		new_prefix[new_prefix_idx++] = other[i];
	}
	// 2) now move the current key as part of the prefix
	new_prefix[new_prefix_idx++] = key;
	// 3) move the existing prefix (if any)
	for (uint32_t i = 0; i < size; i++) {
		new_prefix[new_prefix_idx++] = prefix[i];
	}
	prefix = move(new_prefix);
	size = new_length;
}

uint8_t Prefix::Reduce(uint32_t n) {
	auto new_size = size - n - 1;
	auto new_prefix = unique_ptr<uint8_t[]>(new uint8_t[new_size]);
	auto key = prefix[n];
	for (idx_t i = 0; i < new_size; i++) {
		new_prefix[i] = prefix[i + n + 1];
	}
	prefix = move(new_prefix);
	size = new_size;
	return key;
}

void Prefix::Serialize(duckdb::MetaBlockWriter &writer) {
	writer.Write(size);
	for (idx_t i = 0; i < size; i++) {
		writer.Write(prefix[i]);
	}
}

void Prefix::Deserialize(duckdb::MetaBlockReader &reader) {
	size = reader.Read<uint32_t>();
	prefix = unique_ptr<uint8_t[]>(new uint8_t[size]);
	for (idx_t i = 0; i < size; i++) {
		prefix[i] = reader.Read<uint8_t>();
	}
}

uint32_t Prefix::KeyMismatchPosition(Key &key, uint64_t depth) {
	uint64_t pos;
	for (pos = 0; pos < size; pos++) {
		if (key[depth + pos] != prefix[pos]) {
			return pos;
		}
	}
	return pos;
}

uint32_t Prefix::MismatchPosition(Prefix &other) {

	for (idx_t i = 0; i < size; i++) {
		if (prefix[i] != other[i]) {
			return i;
		}
	}
	return size;
}

} // namespace duckdb
