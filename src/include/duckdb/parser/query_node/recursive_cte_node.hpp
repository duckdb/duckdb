//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/query_node/recursive_cte_node.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/parser/query_node.hpp"
#include "duckdb/parser/sql_statement.hpp"

namespace duckdb {

class RecursiveCTENode : public QueryNode {
public:
	RecursiveCTENode() : QueryNode(QueryNodeType::RECURSIVE_CTE_NODE) {
	}

	string ctename;
	bool union_all;
	//! The left side of the set operation
	unique_ptr<QueryNode> left;
	//! The right side of the set operation
	unique_ptr<QueryNode> right;
	//! Aliases of the recursive CTE node
	vector<string> aliases;

	const vector<unique_ptr<ParsedExpression>> &GetSelectList() const override {
		return left->GetSelectList();
	}

public:
	//! Convert the query node to a string
	string ToString() const override;

	bool Equals(const QueryNode *other) const override;
	//! Create a copy of this SelectNode
	unique_ptr<QueryNode> Copy() const override;

	//! Serializes a QueryNode to a stand-alone binary blob
	void Serialize(FieldWriter &writer) const override;
	//! Deserializes a blob back into a QueryNode
	static unique_ptr<QueryNode> Deserialize(FieldReader &reader);
};

} // namespace duckdb
