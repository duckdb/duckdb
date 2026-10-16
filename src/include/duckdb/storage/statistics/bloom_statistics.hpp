//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/statistics/bloom_statistics.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/vector_size.hpp"

#include <array>

namespace duckdb {

class Serializer;
class Deserializer;
struct SelectionVector;
class Vector;

class BloomIndex;

class BloomColumnFilter {
	friend class BloomIndex;

public:
	static constexpr idx_t BITS_PER_KEY = 8;
	static constexpr idx_t BLOOM_SECTORS = STANDARD_VECTOR_SIZE * BITS_PER_KEY / 64;

public:
	void InsertHashes(const Vector &hashes_v, idx_t count) noexcept;
	idx_t MaybeContains(const Vector &hashes_v, SelectionVector &res_sel, idx_t count) const noexcept;
	void InsertOne(hash_t hash) noexcept;
	bool MaybeContainsOne(hash_t hash) const noexcept;
	void Serialize(Serializer &serializer);
	void Deserialize(Deserializer &deserializer);

private:
	uint64_t GetMask(hash_t hash) const noexcept;
	void Clear() noexcept {
		words.fill(0);
	}

private:
	std::array<uint64_t, BLOOM_SECTORS> words {};
};

} // namespace duckdb
