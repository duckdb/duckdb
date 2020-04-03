//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/filter_pushdown.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/unordered_set.hpp"
#include "duckdb/optimizer/filter_combiner.hpp"
#include "duckdb/optimizer/rule.hpp"

namespace duckdb {

class Optimizer;

class FilterPushdown {
public:
	FilterPushdown(Optimizer &optimizer) : optimizer(optimizer) {
	}
	//! Perform filter pushdown
	unique_ptr<LogicalOperator> Rewrite(unique_ptr<LogicalOperator> node);

	struct Filter {
		unordered_set<idx_t> bindings;
		unique_ptr<Expression> filter;

		Filter() {
		}
		Filter(unique_ptr<Expression> filter) : filter(move(filter)) {
		}

		void ExtractBindings();
	};

private:
	vector<unique_ptr<Filter>> filters;
	Optimizer &optimizer;

	//! Push down a LogicalAggregate op
	unique_ptr<LogicalOperator> PushdownAggregate(unique_ptr<LogicalOperator> op);
	//! Push down a LogicalFilter op
	unique_ptr<LogicalOperator> PushdownFilter(unique_ptr<LogicalOperator> op);
	//! Push down a LogicalCrossProduct op
	unique_ptr<LogicalOperator> PushdownCrossProduct(unique_ptr<LogicalOperator> op);
	//! Push down a join operator
	unique_ptr<LogicalOperator> PushdownJoin(unique_ptr<LogicalOperator> op);
	//! Push down a LogicalProjection op
	unique_ptr<LogicalOperator> PushdownProjection(unique_ptr<LogicalOperator> op);
	//! Push down a LogicalSetOperation op
	unique_ptr<LogicalOperator> PushdownSetOperation(unique_ptr<LogicalOperator> op);
	//! Push down a LogicalGet op
	unique_ptr<LogicalOperator> PushdownGet(unique_ptr<LogicalOperator> op);
	// Pushdown an inner join
	unique_ptr<LogicalOperator> PushdownInnerJoin(unique_ptr<LogicalOperator> op, unordered_set<idx_t> &left_bindings,
	                                              unordered_set<idx_t> &right_bindings);
	// Pushdown a left join
	unique_ptr<LogicalOperator> PushdownLeftJoin(unique_ptr<LogicalOperator> op, unordered_set<idx_t> &left_bindings,
	                                             unordered_set<idx_t> &right_bindings);
	// Pushdown a mark join
	unique_ptr<LogicalOperator> PushdownMarkJoin(unique_ptr<LogicalOperator> op, unordered_set<idx_t> &left_bindings,
	                                             unordered_set<idx_t> &right_bindings);
	// Pushdown a single join
	unique_ptr<LogicalOperator> PushdownSingleJoin(unique_ptr<LogicalOperator> op, unordered_set<idx_t> &left_bindings,
	                                               unordered_set<idx_t> &right_bindings);

	// Finish pushing down at this operator, creating a LogicalFilter to store any of the stored filters and recursively
	// pushing down into its children (if any)
	unique_ptr<LogicalOperator> FinishPushdown(unique_ptr<LogicalOperator> op);
	//! Adds a filter to the set of filters. Returns FilterResult::UNSATISFIABLE if the subtree should be stripped, or
	//! FilterResult::SUCCESS otherwise
	FilterResult AddFilter(unique_ptr<Expression> expr);
	//! Generate filters from the current set of filters stored in the FilterCombiner
	void GenerateFilters();
	//! if there are filters in this FilterPushdown node, push them into the combiner
	void PushFilters();

	FilterCombiner combiner;
};

} // namespace duckdb
