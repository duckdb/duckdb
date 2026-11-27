//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/join_order/join_order_plan.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/optimizer/join_order/join_order_candidate.hpp"

namespace duckdb {

JoinOrderPlanMap CopyJoinOrderLeafPlans(const JoinOrderPlanMap &source);

//! A complete closure for one explicit root, re-costed after capturing all its children.
class CapturedJoinOrderPlan {
public:
	static unique_ptr<CapturedJoinOrderPlan> Capture(const JoinOrderPlanMap &source, JoinRelationSet &root,
	                                                 CostModel &cost_model);
	JoinRelationSet &GetRoot() const;
	const DPJoinNode &GetRootNode() const;
	const JoinOrderPlanMap &GetPlans() const;
	vector<RelationIndex> GetLeafOrder() const;
	JoinOrderPlanMap TakePlans();

private:
	CapturedJoinOrderPlan(JoinRelationSet &root, JoinOrderPlanMap plans);

	JoinRelationSet &root;
	JoinOrderPlanMap plans;
};

} // namespace duckdb
