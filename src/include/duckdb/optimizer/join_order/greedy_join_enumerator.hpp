//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/join_order/greedy_join_enumerator.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/optimizer/join_order/join_order_candidate.hpp"

namespace duckdb {

class GreedyJoinEnumerator {
public:
	GreedyJoinEnumerator(QueryGraphManager &graph, JoinOrderCandidateEvaluator &evaluator);

	//! The driver initializes the input plan and selects the working memo before invoking greedy search.
	//! Return true if all relations were merged into a complete plan, or false if greedy cannot complete the next
	//! merge.
	bool Solve(JoinOrderPlanMap &plans);

private:
	QueryGraphManager &query_graph_manager;
	JoinOrderCandidateEvaluator &evaluator;
};

} // namespace duckdb
