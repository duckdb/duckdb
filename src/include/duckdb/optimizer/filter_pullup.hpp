//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/filter_pullup.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/unordered_set.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include <memory>
#include <vector>

namespace duckdb {

class Optimizer;

class FilterPullup {
public:
    FilterPullup(Optimizer &optimizer) : optimizer(optimizer) {
    }

    FilterPullup(Optimizer &optimizer, unique_ptr<LogicalOperator>::pointer root_proj, bool fork=false) : 
                 optimizer(optimizer),  root_pullup_node_ptr(root_proj), fork(fork) {
    }

    //! Perform filter pullup
    unique_ptr<LogicalOperator> Rewrite(unique_ptr<LogicalOperator> node);

private:
    vector<unique_ptr<Expression>> filters_expr_pullup;
    Optimizer &optimizer;
    // node resposible for pulling up filters
    unique_ptr<LogicalOperator>::pointer root_pullup_node_ptr = nullptr;
    // only pull up filters when there is a fork
    bool fork = false;

    // Generate logical filters pulled up
    unique_ptr<LogicalOperator> GeneratePullupFilter(unique_ptr<LogicalOperator> child, vector<unique_ptr<Expression>> &expressions);

	//! Pull up a LogicalFilter op
	unique_ptr<LogicalOperator> PullupFilter(unique_ptr<LogicalOperator> op);

    //! Pull up filter in a LogicalProjection op
    unique_ptr<LogicalOperator> PullupProjection(unique_ptr<LogicalOperator> op);

    //! Pull up filter in a LogicalCrossProduct op
    unique_ptr<LogicalOperator> PullupCrossProduct(unique_ptr<LogicalOperator> op);

    unique_ptr<LogicalOperator> PullupJoin(unique_ptr<LogicalOperator> op);

    // PPullup filter in a left join
	unique_ptr<LogicalOperator> PullupLeftJoin(unique_ptr<LogicalOperator> op);

    // Pullup filter in a inner join
	unique_ptr<LogicalOperator> PullupInnerJoin(unique_ptr<LogicalOperator> op);

    // Pullup filter in LogicalIntersect op
    unique_ptr<LogicalOperator> PullupIntersect(unique_ptr<LogicalOperator> op);

    unique_ptr<LogicalOperator> PullupBothSide(unique_ptr<LogicalOperator> op);

    // Finish pull up at this operator
	unique_ptr<LogicalOperator> FinishPullup(unique_ptr<LogicalOperator> op);

}; //end FilterPullup

}// namespace duckdb