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
	//! The amount of columns in the result
	size_t result_column_count;

	//! Group By Description
	GroupByDescription groupby;

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
		return result_column_count;
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
