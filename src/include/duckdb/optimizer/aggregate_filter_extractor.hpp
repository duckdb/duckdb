//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/aggregate_filter_extractor.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/optional.hpp"
#include "duckdb/planner/expression.hpp"

namespace duckdb {

class LogicalAggregate;
class Optimizer;

//! Extracts input predicates before an aggregate or its branches are lowered.
class AggregateFilterExtractor {
public:
	//! Construct after staging shared expressions and before building branch inputs.
	AggregateFilterExtractor(Optimizer &optimizer, LogicalAggregate &aggr);

	//! Remove identical filters, or derive single-column input predicates while retaining different filters.
	unique_ptr<Expression> Extract(const vector<idx_t> &aggregate_indices);

private:
	Optimizer &optimizer;
	LogicalAggregate &aggr;
	optional<bool> can_filter_input;
};

} // namespace duckdb
