//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/remove_useless_groups.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/column_binding_map.hpp"
#include "duckdb/planner/logical_operator_visitor.hpp"

namespace duckdb {

class BoundColumnRefExpression;
class TableCatalogEntry;
class ClientContext;

//! The RemoveUselessGroups optimizer traverses the logical operator tree and removes any useless aggregate groups.
//! We only consider the primary key for now. For a table with a primary key, if the columns in the GROUP BY clause
//! encompass all the key values of the primary key, it means the primary key already represents the grouping.
//! Consequently, columns beyond the primary key can be discarded.
//! For example, in the SQL query: SELECT count(*), a FROM t GROUP BY a, b, c; If the column 'a' has a primary key
//! index, the columns 'b' and 'c' in the GROUP BY clause can be removed. This simplifies the query to be equivalent to:
//! SELECT count(*), a FROM t GROUP BY a;
class RemoveUselessGroups : public LogicalOperatorVisitor {
public:
	RemoveUselessGroups(ClientContext &context) : context(context), finish_collection(false) {
	}

	void VisitOperator(LogicalOperator &op) override;

private:
	void VisitAggregate(LogicalAggregate &aggr);
	void CollectPrimaryKeySet(LogicalOperator &op);

protected:
	unique_ptr<Expression> VisitReplace(BoundColumnRefExpression &expr, unique_ptr<Expression> *expr_ptr) override;

private:
	ClientContext &context;
	//! The map of column references
	column_binding_map_t<vector<reference<BoundColumnRefExpression>>> column_references;
	//! Stored expressions (kept around so we don't have dangling pointers)
	vector<unique_ptr<Expression>> stored_expressions;
	unordered_map<idx_t, unordered_set<column_t>> table_primary_key_map;
	bool finish_collection;
};

} // namespace duckdb
