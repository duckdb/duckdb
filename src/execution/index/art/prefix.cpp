#include "duckdb/execution/index/art/prefix.hpp"

namespace duckdb {

uint32_t Prefix::Size() const {
	return sizeof(prefix) / sizeof(prefix[0]);
}

Prefix::Prefix() {
}

Prefix::Prefix(Key &key, uint32_t depth, uint32_t size) {
	// Allocate new prefix
	prefix = unique_ptr<uint8_t[]>(new uint8_t[size]);

	// Copy Key to Prefix
	idx_t prefix_idx = 0;
	for (idx_t i = depth; i < key.len; i++) {
		prefix[prefix_idx++] = key.data[i];
	}
}

uint8_t &Prefix::operator[](idx_t idx) {
	D_ASSERT(idx < Size());
	return prefix[idx];
}

Prefix &Prefix::operator=(const Prefix &src) {
	auto src_size = src.Size();
	// Allocate new prefix
	prefix = unique_ptr<uint8_t[]>(new uint8_t[src_size]);

	// Copy
	for (idx_t i = 0; i < src_size; i++) {
		prefix[i] = src.prefix[i];
	}
	return *this;
}

Prefix &Prefix::operator=(Prefix &&other) noexcept {
	prefix = move(other.prefix);
	return *this;
}

void Prefix::Reduce(uint32_t n) {
	auto cur_size = Size();
	auto new_prefix = unique_ptr<uint8_t[]>(new uint8_t[cur_size - n]);
	for (idx_t i = n; i < cur_size; i++) {
		new_prefix[i - n] = prefix[i];
	}
	prefix = move(new_prefix);
}

void Prefix::Concatenate(uint8_t key, Prefix &other) {
	auto new_length = Size() + 1 + other.Size();
	// have to allocate space in our prefix array
	unique_ptr<uint8_t[]> new_prefix = unique_ptr<uint8_t[]>(new uint8_t[new_length]);
	idx_t new_prefix_idx = 0;
	// 1) Add the to-be deleted Node's prefix
	for (uint32_t i = 0; i < other.Size(); i++) {
		new_prefix[new_prefix_idx++] = other[i];
	}
	// 2) now move the current key as part of the prefix
	new_prefix[new_prefix_idx++] = key;
	// 3) move the existing prefix (if any)
	for (uint32_t i = 0; i < Size(); i++) {
		new_prefix[new_prefix_idx++] = prefix[i];
	}
	prefix = move(new_prefix);
	D_ASSERT(new_prefix_idx == Size());
}

void Prefix::Serialize(duckdb::MetaBlockWriter &writer) {
	auto cur_size = Size();
	writer.Write(cur_size);
	for (idx_t i = 0; i < cur_size; i++) {
		writer.Write(prefix[i]);
	}
}

void Prefix::Deserialize(duckdb::MetaBlockReader &reader) {
	auto prefix_length = reader.Read<uint32_t>();
	prefix = unique_ptr<uint8_t[]>(new uint8_t[prefix_length]);
	for (idx_t i = 0; i < prefix_length; i++) {
		prefix[i] = reader.Read<uint8_t>();
	}
}

uint32_t Prefix::KeyMismatch(Key &key, uint64_t depth) {
	uint64_t pos;
	auto cur_size = Size();
	for (pos = 0; pos < cur_size; pos++) {
		if (key[depth + pos] != prefix[pos]) {
			return pos;
		}
	}
	return pos;
}

bool Prefix::EqualKey(Key &key, unsigned depth) {
	auto prefix_length = Size();
	for (idx_t i = 0; i < prefix_length; i++) {
		if (prefix[i] != key.data[i + depth]) {
			return false;
		}
	}
	return true;
}

bool Prefix::GTKey(Key &key, unsigned depth) {
	auto prefix_length = Size();
	for (idx_t i = 0; i < prefix_length; i++) {
		if (prefix[i] > key.data[i + depth]) {
			return true;
		} else if (prefix[i] < key.data[i + depth]) {
			return false;
		}
	}
	return false;
}

bool Prefix::GTEKey(Key &key, unsigned depth) {
	auto prefix_length = Size();
	for (idx_t i = 0; i < prefix_length; i++) {
		if (prefix[i] > key.data[i + depth]) {
			return true;
		} else if (prefix[i] < key.data[i + depth]) {
			return false;
		}
	}
	return true;
}

} // namespace duckdb
