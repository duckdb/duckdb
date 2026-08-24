//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/redundant_distinct_remover.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

class Optimizer;

//! Removes DISTINCT operators whose duplicates are already discarded by an operator above them, such as
//! the branches of UNION, INTERSECT and EXCEPT, which are defined on sets.
class RedundantDistinctRemover {
public:
	explicit RedundantDistinctRemover(Optimizer &optimizer);

	unique_ptr<LogicalOperator> Optimize(unique_ptr<LogicalOperator> op);

private:
	//! `deduplicated` says that every duplicate this subtree emits is discarded further up, so a DISTINCT
	//! inside it cannot change the result.
	unique_ptr<LogicalOperator> Visit(unique_ptr<LogicalOperator> op, bool deduplicated);

private:
	Optimizer &optimizer;
};

} // namespace duckdb
