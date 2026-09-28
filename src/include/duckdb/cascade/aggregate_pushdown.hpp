//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/cascade/aggregate_pushdown.hpp
//
// Pushing a GroupBy below a plain inner join: section 3.1 of Galindo-Legaria &
// Joshi, "Orthogonal Optimization of Subqueries and Aggregation" (SIGMOD 2001).
//
//     G_{A,F}( S join_p R )  =  S join_p G_{A - cols(S), F}( R )
//
// This is the direction that *removes* the aggregation above the join, so the join
// reads the groups instead of the rows. Like the pull-up it is a cost decision, and
// the paper's three conditions say when it is an identity at all:
//
//  1. the predicate's columns on the aggregated side have to be grouping columns, or
//     the predicate cannot be evaluated once that side has been aggregated;
//  2. the join has to pick at most one row from the other side per group - otherwise
//     the join above would multiply the group. A unique constraint of that side,
//     equated by the predicate to values the group determines, is what guarantees it;
//  3. the aggregate functions have to read only the aggregated side.
//
// There is also the empty-grouping case to refuse: an aggregate with no grouping at
// all returns one row even for empty input, and the join below it cannot.
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/planner/column_binding.hpp"

namespace duckdb {

class Binder;
class ClientContext;
class LogicalOperator;

class AggregatePushdown {
public:
	AggregatePushdown(Binder &binder, ClientContext &context);

	//! Push every GroupBy that sits above an inner join below it, when the conditions of
	//! section 3.1 hold.
	unique_ptr<LogicalOperator> Push(unique_ptr<LogicalOperator> plan);

private:
	unique_ptr<LogicalOperator> PushNode(unique_ptr<LogicalOperator> op,
	                                     vector<std::pair<ColumnBinding, ColumnBinding>> &exports);

private:
	Binder &binder;
	ClientContext &context;
};

} // namespace duckdb
