//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/cascade/groupby_reorder.hpp
//
// Reordering GroupBy with the operators around it: section 3.1 of
// Galindo-Legaria & Joshi, "Orthogonal Optimization of Subqueries and
// Aggregation" (SIGMOD 2001).
//
// A GroupBy computes one output row per group, so a predicate above it either
// accepts or rejects a whole group. That is what makes a predicate above a GroupBy
// movable below it - and worth moving, because the aggregate then sees fewer rows.
// The paper states the condition precisely: the predicate may move around the
// GroupBy "if and only if all the columns used in the filter are functionally
// determined by the grouping columns in the input relation".
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"

namespace duckdb {

class LogicalOperator;

//! Move the predicates that are constant within a group below the GroupBy they sit
//! above. Predicates that read an aggregate result (`HAVING count(*) > 1`) stay
//! where they are: the aggregate does not exist yet below the GroupBy. This half is
//! always applied - it is what DuckDB's own FilterPushdown::PushdownAggregate does.
//!
//! `move_semijoins` additionally moves the semijoin/antijoin that consumes a GroupBy
//! below it. The paper derives that from the filter case, but it is a cost decision
//! it explicitly hands to the optimizer: joining first can reduce the cardinality
//! that gets aggregated. Measured with section31_perf.py, it is a 1.08x loss on a
//! semijoin that keeps every row and a 0.94x win on one that keeps 0.5%.
//!
//! Moving a GroupBy below an ordinary *join* is the remaining half of section 3.1,
//! and is not done here: without cost information it would be a guess.
unique_ptr<LogicalOperator> ReorderGroupBy(unique_ptr<LogicalOperator> plan, bool move_semijoins);

} // namespace duckdb
