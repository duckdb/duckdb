//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/distinct_on_hash_join_build.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

//! Fuses a DISTINCT ON subquery on the build side of a hash join into a
//! deduplicated hash table build (first row wins per key), eliminating the
//! separate HASH_GROUP_BY pipeline breaker.
class DistinctOnHashJoinBuildOptimizer {
public:
	DistinctOnHashJoinBuildOptimizer() = default;

	unique_ptr<LogicalOperator> Optimize(unique_ptr<LogicalOperator> op);
};

} // namespace duckdb
