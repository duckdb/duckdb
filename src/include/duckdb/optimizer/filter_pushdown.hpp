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
	explicit FilterPushdown(Optimizer &optimizer, bool convert_mark_joins = true);

	//! Perform filter pushdown
	unique_ptr<LogicalOperator> Rewrite(unique_ptr<LogicalOperator> op);
	//! Return a reference to the client context (from the optimizer)
	ClientContext &GetContext();

	void CheckMarkToSemi(LogicalOperator &op, unordered_set<idx_t> &table_bindings);

	struct Filter {
		unordered_set<idx_t> bindings;
		unique_ptr<Expression> filter;

		Filter() {
		}
		explicit Filter(unique_ptr<Expression> filter) : filter(std::move(filter)) {
		}

		void ExtractBindings();
	};

private:
	Optimizer &optimizer;
	FilterCombiner combiner;
	bool convert_mark_joins;

	vector<unique_ptr<Filter>> filters;
	//! Push down a LogicalAggregate op
	unique_ptr<LogicalOperator> PushdownAggregate(unique_ptr<LogicalOperator> op);
	//! Push down a distinct operator
	unique_ptr<LogicalOperator> PushdownDistinct(unique_ptr<LogicalOperator> op);
	//! Push down a LogicalFilter op
	unique_ptr<LogicalOperator> PushdownFilter(unique_ptr<LogicalOperator> op);
	//! Push down a LogicalCrossProduct op
	unique_ptr<LogicalOperator> PushdownCrossProduct(unique_ptr<LogicalOperator> op);
	//! Push down a join operator
	unique_ptr<LogicalOperator> PushdownJoin(unique_ptr<LogicalOperator> op);
	//! Push down a LogicalProjection op
	unique_ptr<LogicalOperator> PushdownProjection(unique_ptr<LogicalOperator> op);
	//! Push down a LogicalProjection op
	unique_ptr<LogicalOperator> PushdownUnnest(unique_ptr<LogicalOperator> op);
	//! Push down a LogicalSetOperation op
	unique_ptr<LogicalOperator> PushdownSetOperation(unique_ptr<LogicalOperator> op);
	//! Push down a LogicalGet op
	unique_ptr<LogicalOperator> PushdownGet(unique_ptr<LogicalOperator> op);
	//! Push down a LogicalLimit op
	unique_ptr<LogicalOperator> PushdownLimit(unique_ptr<LogicalOperator> op);
	//! Push down a LogicalWindow op
	unique_ptr<LogicalOperator> PushdownWindow(unique_ptr<LogicalOperator> op);
	// Pushdown an inner join
	unique_ptr<LogicalOperator> PushdownInnerJoin(unique_ptr<LogicalOperator> op, unordered_set<idx_t> &left_bindings,
	                                              unordered_set<idx_t> &right_bindings);
	// Pushdown a left join
	unique_ptr<LogicalOperator> PushdownLeftJoin(unique_ptr<LogicalOperator> op, unordered_set<idx_t> &left_bindings,
	                                             unordered_set<idx_t> &right_bindings);

	// Pushdown an outer join
	unique_ptr<LogicalOperator> PushdownOuterJoin(unique_ptr<LogicalOperator> op, unordered_set<idx_t> &left_bindings,
	                                              unordered_set<idx_t> &right_bindings);
	unique_ptr<LogicalOperator> PushdownSemiAntiJoin(unique_ptr<LogicalOperator> op);
	// Pushdown a mark join
	unique_ptr<LogicalOperator> PushdownMarkJoin(unique_ptr<LogicalOperator> op, unordered_set<idx_t> &left_bindings,
	                                             unordered_set<idx_t> &right_bindings);
	// Pushdown a single join
	unique_ptr<LogicalOperator> PushdownSingleJoin(unique_ptr<LogicalOperator> op, unordered_set<idx_t> &left_bindings,
	                                               unordered_set<idx_t> &right_bindings);

	// AddLogicalFilter used to add an extra LogicalFilter at this level,
	// because in some cases, some expressions can not be pushed down.
	unique_ptr<LogicalOperator> AddLogicalFilter(unique_ptr<LogicalOperator> op,
	                                             vector<unique_ptr<Expression>> expressions);
	//! Push any remaining filters into a LogicalFilter at this level
	unique_ptr<LogicalOperator> PushFinalFilters(unique_ptr<LogicalOperator> op);
	// Finish pushing down at this operator, creating a LogicalFilter to store any of the stored filters and recursively
	// pushing down into its children (if any)
	unique_ptr<LogicalOperator> FinishPushdown(unique_ptr<LogicalOperator> op);
	//! Adds a filter to the set of filters. Returns FilterResult::UNSATISFIABLE if the subtree should be stripped, or
	//! FilterResult::SUCCESS otherwise

	unique_ptr<LogicalOperator> PushFiltersIntoDelimJoin(unique_ptr<LogicalOperator> op);
	FilterResult AddFilter(unique_ptr<Expression> expr);
	//! Extract filter bindings to compare them with expressions in an operator and determine if the filter
	//! can be pushed down
	void ExtractFilterBindings(const Expression &expr, vector<ColumnBinding> &bindings);
	//! Generate filters from the current set of filters stored in the FilterCombiner
	void GenerateFilters();
	//! if there are filters in this FilterPushdown node, push them into the combiner. Returns
	//! FilterResult::UNSATISFIABLE if the subtree should be stripped, or FilterResult::SUCCESS otherwise
	FilterResult PushFilters();
};

} // namespace duckdb
