//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/query_node/select_node.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/parser/query_node.hpp"
#include "duckdb/parser/sql_statement.hpp"
#include "duckdb/parser/tableref.hpp"

namespace duckdb {

//! SelectNode represents a standard SELECT statement
class SelectNode : public QueryNode {
public:
	SelectNode() : QueryNode(QueryNodeType::SELECT_NODE) {
	}

	//! The projection list
	vector<unique_ptr<ParsedExpression>> select_list;
	//! The FROM clause
	unique_ptr<TableRef> from_table;
	//! The WHERE clause
	unique_ptr<ParsedExpression> where_clause;
	//! list of groups
	vector<unique_ptr<ParsedExpression>> groups;
	//! HAVING clause
	unique_ptr<ParsedExpression> having;

	const vector<unique_ptr<ParsedExpression>> &GetSelectList() const override {
		return select_list;
	}

public:
	bool Equals(const QueryNode *other) const override;
	//! Create a copy of this SelectNode
	unique_ptr<QueryNode> Copy() override;
	//! Serializes a SelectNode to a stand-alone binary blob
	void Serialize(Serializer &serializer) override;
	//! Deserializes a blob back into a SelectNode
	static unique_ptr<QueryNode> Deserialize(Deserializer &source);
};
}; // namespace duckdb
