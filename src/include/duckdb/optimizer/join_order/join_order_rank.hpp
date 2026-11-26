//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/join_order/join_order_rank.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/optimizer/join_order/relation_index.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

//! ASI ranking proxy (q, w), stored logarithmically. This is not the plan cost comparator.
struct JoinOrderRank {
	explicit JoinOrderRank(double log_multiplier);
	void Append(const JoinOrderRank &other);
	void AddIndependentWork(double log_work);
	bool operator<(const JoinOrderRank &other) const;

	double log_multiplier;
	double log_work;

private:
	static double AddLogValues(double left, double right);
	int32_t Sign() const;
	double LogMagnitude() const;
};

struct RankedJoinOrder {
	vector<RelationIndex> relations;
	double log_cost;
};

} // namespace duckdb
