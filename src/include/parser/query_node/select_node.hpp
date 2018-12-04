
#pragma once

#include "parser/query_node.hpp"
#include "parser/expression.hpp"
#include "parser/sql_statement.hpp"
#include "parser/tableref.hpp"
#include "parser/sql_node_visitor.hpp"

namespace duckdb {

//! GROUP BY description
struct GroupByDescription {
	//! List of groups
	std::vector<std::unique_ptr<Expression>> groups;
	//! HAVING clause
	std::unique_ptr<Expression> having;
};

//! SelectNode represents a standard SELECT statement
class SelectNode : public QueryNode {
public:
	SelectNode() : QueryNode(QueryNodeType::SELECT_NODE) {
	}

	void Accept(SQLNodeVisitor *v) override {
		v->Visit(*this);
	}

	//! The projection list
	std::vector<std::unique_ptr<Expression>> select_list;
	//! The FROM clause
	std::unique_ptr<TableRef> from_table;
	//! The WHERE clause
	std::unique_ptr<Expression> where_clause;
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

    vector<unique_ptr<Expression>>& GetSelectList() override { return select_list; }
    
	bool Equals(const QueryNode *other) override;
	//! Create a copy of this SelectNode
	std::unique_ptr<QueryNode> Copy() override;
	//! Serializes a SelectNode to a stand-alone binary blob
	void Serialize(Serializer &serializer) override;
	//! Deserializes a blob back into a SelectNode
	static std::unique_ptr<QueryNode> Deserialize(Deserializer &source);
};
};
