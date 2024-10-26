//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/join_order/join_order_optimizer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/optimizer/join_order/cardinality_estimator.hpp"
#include "duckdb/optimizer/join_order/join_node.hpp"
#include "duckdb/optimizer/join_order/join_relation.hpp"
#include "duckdb/optimizer/join_order/query_graph.hpp"
#include "duckdb/optimizer/join_order/query_graph_manager.hpp"
#include "duckdb/parser/expression_map.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/logical_operator_visitor.hpp"

#include <functional>

namespace duckdb {

class JoinOrderOptimizer {
public:
	explicit JoinOrderOptimizer(ClientContext &context);
	JoinOrderOptimizer CreateChildOptimizer();

public:
	//! Perform join reordering inside a plan
	unique_ptr<LogicalOperator> Optimize(unique_ptr<LogicalOperator> plan, optional_ptr<RelationStats> stats = nullptr);
	//! Adds/gets materialized CTE stats
	void AddMaterializedCTEStats(idx_t index, RelationStats &&stats);
	RelationStats GetMaterializedCTEStats(idx_t index);
	//! Adds/gets delim scan stats
	void AddDelimScanStats(RelationStats &stats);
	RelationStats GetDelimScanStats();

private:
	ClientContext &context;

	//! manages the query graph, relations, and edges between relations
	QueryGraphManager query_graph_manager;

	//! The set of filters extracted from the query graph
	vector<unique_ptr<Expression>> filters;
	//! The set of filter infos created from the extracted filters
	vector<unique_ptr<FilterInfo>> filter_infos;
	//! A map of all expressions a given expression has to be equivalent to. This is used to add "implied join edges".
	//! i.e. in the join A=B AND B=C, the equivalence set of {B} is {A, C}, thus we can add an implied join edge {A = C}
	expression_map_t<vector<FilterInfo *>> equivalence_sets;

	CardinalityEstimator cardinality_estimator;

	unordered_set<std::string> join_nodes_in_full_plan;

	//! Mapping from materialized CTE index to stats
	unordered_map<idx_t, RelationStats> materialized_cte_stats;
	//! Stats of Delim Scans of the Delim Join that is currently being optimized
	optional_ptr<RelationStats> delim_scan_stats;
};

} // namespace duckdb
