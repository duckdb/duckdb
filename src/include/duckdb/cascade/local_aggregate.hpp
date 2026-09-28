//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/cascade/local_aggregate.hpp
//
// LocalGroupBy: section 3.3 of Galindo-Legaria & Joshi, "Orthogonal Optimization
// of Subqueries and Aggregation" (SIGMOD 2001).
//
//     G_{A,F} R  =  G_{A,F_g} LG_{A,F_l} R
//
// Every aggregate function that an engine can spill to disk or evaluate in parallel
// already has to be splittable into a local part and a global part - that is what
// makes the intermediate results of a partition combine into the right answer. The
// paper's observation is that the same split can be used as an optimization: the
// local aggregation reduces the rows that reach the join, and because the global
// aggregation recombines the parts, the local grouping columns can be extended
// freely.
//
// That freedom is what makes this rule applicable where the plain GroupBy pushdown
// of section 3.1 is not. Moving the *whole* GroupBy below a join also removes the
// global aggregation, so each outer row has to be its own group - which is why that
// rule needs "the key of S is part of the grouping columns". Keeping the global
// aggregation makes the move unconditional: two outer rows that share a group
// contribute the same local parts twice, exactly as they contributed their inner
// rows twice before.
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/planner/column_binding.hpp"

namespace duckdb {

class Binder;
class ClientContext;
class LogicalOperator;

class LocalAggregatePusher {
public:
	LocalAggregatePusher(Binder &binder, ClientContext &context);

	//! Push a local aggregate below every join where it can be split.
	unique_ptr<LogicalOperator> Push(unique_ptr<LogicalOperator> plan);

private:
	unique_ptr<LogicalOperator> PushNode(unique_ptr<LogicalOperator> op,
	                                     vector<std::pair<ColumnBinding, ColumnBinding>> &exports);

private:
	Binder &binder;
	ClientContext &context;
};

} // namespace duckdb
