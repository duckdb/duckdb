//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/cascade/aggregate_pullup.hpp
//
// Pulling a GroupBy above a join: section 3.1 of Galindo-Legaria & Joshi,
// "Orthogonal Optimization of Subqueries and Aggregation" (SIGMOD 2001), and the
// primitive section 3.4.2 uses to push a join below a SegmentApply.
//
//     S join_p (G_{A,F} R)  =  G_{A u columns(S), F}( S join_p R )
//
// The paper calls this "a lot easier" than the other direction, and the conditions say
// why: "All that is required is that the relation being joined has a key and that the
// join predicate does not use the results of the aggregate functions." The key is what
// makes the grouping by A u columns(S) land on exactly one outer row per group - with
// duplicate rows on the kept side, two of them would collapse into one group and the
// aggregate would see both their rows where the original saw them separately.
//
// The direction is the opposite of section 3.3: there the aggregate moves down to
// reduce what the join reads, here it moves up, so the join gets to reduce what is
// aggregated. Both are cost decisions, which is why this one is off by default.
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/planner/column_binding.hpp"

namespace duckdb {

class Binder;
class ClientContext;
class LogicalOperator;

class AggregatePullup {
public:
	AggregatePullup(Binder &binder, ClientContext &context);

	//! Pull every GroupBy that sits below an inner join above it, when the conditions
	//! of section 3.1 hold.
	unique_ptr<LogicalOperator> Pull(unique_ptr<LogicalOperator> plan);

private:
	unique_ptr<LogicalOperator> PullNode(unique_ptr<LogicalOperator> op,
	                                     vector<std::pair<ColumnBinding, ColumnBinding>> &exports);

private:
	Binder &binder;
	ClientContext &context;
};

} // namespace duckdb
