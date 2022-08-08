//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/index/art/prefix.hpp
//
//
//===----------------------------------------------------------------------===//
#pragma once

#include "duckdb/execution/index/art/node.hpp"

namespace duckdb {
class Prefix {
public:
	Prefix();
	// Prefix created from key starting on `depth`.
	Prefix(Key &key, uint32_t depth, uint32_t size);

	// Returns the Prefix's size
	uint32_t Size() const;

	// Subscript operator
	uint8_t &operator[](idx_t idx);

	// Assign operator
	Prefix &operator=(const Prefix &src);

	// Move operator
	Prefix &operator=(Prefix &&other) noexcept;

	// Concatenate Prefix with a key and another prefix
	// Used when deleting a Node.
	// other.prefix + key + this->Prefix
	void Concatenate(uint8_t key, Prefix &other);
	// Reduces the prefix in n elements
	void Reduce(uint32_t n);
	// Serializes Prefix
	void Serialize(duckdb::MetaBlockWriter &writer);
	// Deserializes Prefix
	void Deserialize(duckdb::MetaBlockReader &reader);
	// If prefix equals key (starting from depth)
	bool EqualKey(Key &key, unsigned depth);
	// If prefix > key (starting from depth)
	bool GTKey(Key &key, unsigned depth);
	// If prefix >= key (starting from depth)
	bool GTEKey(Key &key, unsigned depth);

	// Compare the key with the prefix of the node, return the number matching bytes
	uint32_t KeyMismatch(Key &key, uint64_t depth);

private:
	unique_ptr<uint8_t[]> prefix;
};

} // namespace duckdb
