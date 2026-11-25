//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parallel/sharded_counter.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/array.hpp"
#include "duckdb/common/atomic.hpp"
#include "duckdb/common/common.hpp"

namespace duckdb {

//! A counter that spreads concurrent updates across estimated CPU groups.
class ShardedCounter {
public:
	void Add(idx_t count);
	//! The total is exact once all writers have stopped.
	idx_t GetTotal() const;
	//! Requires all writers to have stopped.
	void Reset();

private:
	static constexpr idx_t SHARD_COUNT = 16;
	static constexpr idx_t CPUS_PER_SHARD = 16;

	array<PaddedAtomic<idx_t>, SHARD_COUNT> shards {};
};

} // namespace duckdb
