//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/index/art/prefix.hpp
//
//
//===----------------------------------------------------------------------===//
#pragma once

#include "duckdb/execution/index/art/art_key.hpp"
#include "duckdb/storage/meta_block_reader.hpp"
#include "duckdb/storage/meta_block_writer.hpp"

namespace duckdb {
class ART;

class Prefix {
	static constexpr idx_t PREFIX_INLINE_BYTES = 8;

public:
	//! Empty prefix
	Prefix();
	//! Construct prefix from key starting at depth
	Prefix(Key &key, uint32_t depth, uint32_t size);
	//! Construct prefix from other prefix up to size
	Prefix(Prefix &other_prefix, uint32_t size);
	~Prefix();

	//! Returns the prefix size
	inline uint32_t Size() const {
		return size;
	}
	//! Sets the size of a prefix to 0
	void SetEmpty();
	//! Returns the memory size of the prefix
	idx_t MemorySize();
	//! Returns a pointer to the prefix data
	uint8_t *GetPrefixData();
	//! Returns a const pointer to the prefix data
	const uint8_t *GetPrefixData() const;

	//! Subscript operator
	uint8_t &operator[](idx_t idx);
	//! Assign operator
	Prefix &operator=(const Prefix &src);
	//! Move operator
	Prefix &operator=(Prefix &&other) noexcept;

	//! Concatenate prefix with a partial key byte and another prefix: other.prefix + byte + this->prefix
	//! Used when deleting a node
	void Concatenate(ART &art, uint8_t key, Prefix &other);
	//! Reduces the prefix in n bytes, and returns the new first byte
	uint8_t Reduce(ART &art, uint32_t n);

	//! Serializes the prefix
	void Serialize(duckdb::MetaBlockWriter &writer);
	//! Deserializes the prefix
	void Deserialize(duckdb::MetaBlockReader &reader);

	//! Compare the key with the prefix of the node, return the position where they mismatch
	uint32_t KeyMismatchPosition(Key &key, uint32_t depth);
	//! Compare this prefix to another prefix, return the position where they mismatch, or size otherwise
	uint32_t MismatchPosition(Prefix &other);

private:
	uint32_t size;
	union {
		uint8_t *ptr;
		uint8_t inlined[8];
	} value;

private:
	bool IsInlined() const;
	uint8_t *AllocatePrefix(uint32_t size);
	void Overwrite(uint32_t new_size, uint8_t *data);
	void Destroy();
};

} // namespace duckdb
