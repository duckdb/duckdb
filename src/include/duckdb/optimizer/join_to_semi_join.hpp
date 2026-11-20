//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/join_to_semi_join.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/table_index.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/unordered_set.hpp"

namespace duckdb {

class ClientContext;
class JoinRegionRewriter;
class LogicalOperator;

//! The region rewrite of JoinSimplification: in a region of filters, cross products and INNER joins, turns the join
//! against a relation that is only read by predicates comparing it with one other relation into a SEMI join on that
//! relation, when the rows the join repeats do not matter. Collects the region and relocates its predicates.
//! See join_to_semi_join.cpp for the conditions.
class JoinToSemiJoin {
public:
	explicit JoinToSemiJoin(ClientContext &context);
	~JoinToSemiJoin();

	//! Looks at the CTEs of the plan; to be called once, before the traversal
	void Initialize(LogicalOperator &plan);
	//! A node of a join region: a filter, a cross product or a plain INNER join
	static bool IsRegionNode(const LogicalOperator &op);
	//! Applies one rewrite to the region rooted at `op` and returns whether it did. `ignores_duplicates`: the
	//! operators above give the same result over the rows of the region with or without repeats; `read_tables`: the
	//! tables they read through expressions
	bool RewriteRegion(unique_ptr<LogicalOperator> &op, bool ignores_duplicates,
	                   const unordered_set<TableIndex> &read_tables);
	//! To be called for a materialized CTE before its references are visited
	void VisitMaterializedCTE(const LogicalOperator &op);

	//! The tables the expressions of `op` read
	static void AddReadTables(const LogicalOperator &op, unordered_set<TableIndex> &tables);
	static bool HasVolatileExpression(const LogicalOperator &op);

private:
	unique_ptr<JoinRegionRewriter> rewriter;
};

} // namespace duckdb
