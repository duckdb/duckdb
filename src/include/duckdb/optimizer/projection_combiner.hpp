//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/projection_combiner.hpp
//
//
//===----------------------------------------------------------------------===//
#pragma once

#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

class ProjectionCombiner {
public:
	ProjectionCombiner() {
	}

	//! Combine projection into the operator below (if possible)
	unique_ptr<LogicalOperator> Optimize(unique_ptr<LogicalOperator> op);
};

} // namespace duckdb