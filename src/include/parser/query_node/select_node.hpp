//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parser/query_node/select_node.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/expression.hpp"
#include "parser/query_node.hpp"
#include "parser/sql_node_visitor.hpp"
#include "parser/sql_statement.hpp"
#include "parser/tableref.hpp"

namespace duckdb {

//! GROUP BY description
struct GroupByDescription {
	//! List of groups
	vector<unique_ptr<Expression>> groups;
	//! HAVING clause
	unique_ptr<Expression> having;
};

//! SelectNode represents a standard SELECT statement
class SelectNode : public QueryNode {
public:
	SelectNode() : QueryNode(QueryNodeType::SELECT_NODE) {
	}

	//! The projection list
	vector<unique_ptr<Expression>> select_list;
	//! The FROM clause
	unique_ptr<TableRef> from_table;
	//! The WHERE clause
	unique_ptr<Expression> where_clause;
	//! Group By Description
	GroupByDescription groupby;

	//! The following information is only gathered after binding the SelectNode
	struct {
		//! Whether or not the statement has an aggregation
		bool has_aggregation;
		//! Whether or not the statement has a WINDOW function
		bool has_window;
		//! The amount of columns in the final result
		size_t column_count;
		//! Index used by the LogicalProjection
		size_t projection_index;

		//! Group index used by the LogicalAggregate (only used if HasAggregation is true)
		size_t group_index;
		//! Aggregate index used by the LogicalAggregate (only used if HasAggregation is true)
		size_t aggregate_index;
		//! Aggregate functions to compute (only used if HasAggregation is true)
		vector<unique_ptr<Expression>> aggregates;

		//! Window index used by the LogicalWindow (only used if HasWindow is true)
		size_t window_index;
		//! Window functions to compute (only used if HasWindow is true)
		vector<unique_ptr<Expression>> windows;
	} binding;

	//! Whether or not the query has a GROUP BY clause
	bool HasGroup() {
		return groupby.groups.size() > 0;
	}
	//! Whether or not the query has a HAVING clause
	bool HasHaving() {
		return groupby.having.get();
	}
	//! Whether or not the query has an AGGREGATION
	bool HasAggregation();

	//! Whether or not the query has a window function
	bool HasWindow();


	vector<unique_ptr<Expression>> &GetSelectList() override {
		return select_list;
	}

	size_t GetSelectCount() override {
		return binding.column_count;
	}

	bool Equals(const QueryNode *other) const override;
	//! Create a copy of this SelectNode
	unique_ptr<QueryNode> Copy() override;
	//! Serializes a SelectNode to a stand-alone binary blob
	void Serialize(Serializer &serializer) override;
	//! Deserializes a blob back into a SelectNode
	static unique_ptr<QueryNode> Deserialize(Deserializer &source);
};
}; // namespace duckdb
