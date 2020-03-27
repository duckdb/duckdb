//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/predicate_pushdown.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/optimizer/filter_combiner.hpp"
#include "duckdb/optimizer/rule.hpp"

namespace duckdb {
class Optimizer;

class PredicatePushdown {
public:
    //! Try to pushdown filter predicates to table scan node
    unique_ptr<LogicalOperator> Optimize(unique_ptr<LogicalOperator> node);
private:
    //! Transform a Filter in an index scan
    unique_ptr<LogicalOperator> PushdownFilterPredicates(unique_ptr<LogicalOperator> op);
};

} // namespace duckdb
