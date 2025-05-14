//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/cte_inlining.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/insertion_order_preserving_map.hpp"
#include "duckdb/planner/bound_parameter_map.hpp"

namespace duckdb {

class LogicalOperator;
class Optimizer;
struct BoundParameterData;

class CTEInlining {
public:
	explicit CTEInlining(Optimizer &optimizer);
	unique_ptr<LogicalOperator> Optimize(unique_ptr<LogicalOperator> op);

private:
	void TryInlining(unique_ptr<LogicalOperator> &op);
	bool Inline(unique_ptr<LogicalOperator> &op, LogicalOperator &materialized_cte, bool requires_copy = true);

private:
	//! The optimizer
	Optimizer &optimizer;

	optional_ptr<bound_parameter_map_t> parameter_data;
};

} // namespace duckdb
