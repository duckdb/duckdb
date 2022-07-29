#include "duckdb/execution/index/art/prefix.hpp"

namespace duckdb {

uint32_t Prefix::Size() {
	return sizeof(prefix) / sizeof(prefix[0]);
}

Prefix::Prefix(uint32_t prefix_length) {
	prefix = unique_ptr<uint8_t[]>(new uint8_t[prefix_length]);
}

uint8_t &Prefix::operator[](idx_t idx) {
	D_ASSERT(idx < Size());
	return prefix[idx];
}

void Prefix::operator=(Prefix &src) {
	auto src_size = src.Size();
	// Allocate new prefix
	prefix = unique_ptr<uint8_t[]>(new uint8_t[src_size]);

	// Copy
	for (idx_t i = 0; i < src_size; i++) {
		prefix[i] = src[i];
	}
}

void Prefix::Reduce(uint32_t n) {
	auto cur_size = Size();
	auto new_prefix = unique_ptr<uint8_t[]>(new uint8_t[cur_size - n]);
	for (idx_t i = n; i < cur_size; i++) {
		new_prefix[i - n] = prefix[i];
	}
	prefix = move(new_prefix);
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
