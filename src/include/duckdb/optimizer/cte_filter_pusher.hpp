//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/cte_filter_pusher.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/insertion_order_preserving_map.hpp"
#include "duckdb/planner/column_binding.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/enums/expression_type.hpp"

namespace duckdb {

class LogicalOperator;
class LogicalCTERef;
class Optimizer;

class CTEFilterPusher {
public:
	explicit CTEFilterPusher(Optimizer &optimizer);
	//! Push consumer predicates and eligible join-key restrictions into materialized CTEs.
	unique_ptr<LogicalOperator> Optimize(unique_ptr<LogicalOperator> op);
	//! Discard planner guarantees before transformations that do not preserve consumer identity.
	static void ClearDependencies(LogicalOperator &op);

private:
	friend class FilterPushdown;
	void AddJoinFilter(const LogicalCTERef &source, const LogicalCTERef &target,
	                   const vector<ColumnBinding> &source_keys, const vector<ColumnBinding> &target_keys,
	                   const vector<ExpressionType> &comparisons);

private:
	//! CTE info needed for creating OR filters that can be pushed down
	struct MaterializedCTEInfo {
		explicit MaterializedCTEInfo(LogicalOperator &materialized_cte);
		LogicalOperator &materialized_cte;
		vector<reference<LogicalOperator>> filters;
		vector<reference<LogicalCTERef>> references;
		bool all_cte_refs_are_filtered;
		bool has_filter_dependency;
		vector<TableIndex> ancestors;
	};

private:
	//! Find all materialized CTEs and their refs
	void FindCandidates(LogicalOperator &op);
	bool CanPushFilter(const MaterializedCTEInfo &info);
	bool HasValidDependency(const MaterializedCTEInfo &info);
	unique_ptr<LogicalOperator> PushJoinFilters(unique_ptr<LogicalOperator> op);

	struct JoinFilter {
		TableIndex source;
		TableIndex target;
		TableIndex target_scan;
		vector<ProjectionIndex> source_columns;
		vector<ProjectionIndex> target_columns;
		vector<ExpressionType> comparisons;
	};
	//! Creates an OR filter and pushes it into a materialized CTE
	void PushFilterIntoCTE(MaterializedCTEInfo &info);

private:
	//! The optimizer
	Optimizer &optimizer;
	//! Mapping from CTE index to CTE info, order preserving so deepest CTEs are done first
	InsertionOrderPreservingMap<unique_ptr<MaterializedCTEInfo>> cte_info_map;
	vector<TableIndex> available_ctes;
	vector<JoinFilter> join_filters;
	unordered_map<TableIndex, TableIndex> join_targets;
};

} // namespace duckdb
