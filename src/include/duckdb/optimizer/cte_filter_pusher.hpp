//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/cte_filter_pusher.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/insertion_order_preserving_map.hpp"

namespace duckdb {

class LogicalOperator;
class Optimizer;

class CTEFilterPusher {
public:
	explicit CTEFilterPusher(Optimizer &optimizer);
	//! Finds all materialized CTEs and pushes OR filters into them (if applicable)
	unique_ptr<LogicalOperator> Optimize(unique_ptr<LogicalOperator> op);

private:
	//! CTE info needed for creating OR filters that can be pushed down
	struct MaterializedCTEInfo {
		explicit MaterializedCTEInfo(LogicalOperator &materialized_cte);
		LogicalOperator &materialized_cte;
		vector<reference<LogicalOperator>> filters;
		bool all_cte_refs_are_filtered;
	};

private:
	//! Find all materialized CTEs and their refs
	void FindCandidates(LogicalOperator &op);
	//! Creates an OR filter and pushes it into a materialized CTE
	void PushFilterIntoCTE(MaterializedCTEInfo &info);

private:
	//! The optimizer
	Optimizer &optimizer;
	//! Mapping from CTE index to CTE info, order preserving so deepest CTEs are done first
	InsertionOrderPreservingMap<unique_ptr<MaterializedCTEInfo>> cte_info_map;
};

} // namespace duckdb
