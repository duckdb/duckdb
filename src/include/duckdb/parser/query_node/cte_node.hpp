//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/query_node/cte_node.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/parser/query_node.hpp"
#include "duckdb/parser/sql_statement.hpp"

namespace duckdb {

class CTENode : public QueryNode {
public:
	static constexpr const QueryNodeType TYPE = QueryNodeType::CTE_NODE;

public:
	CTENode() : QueryNode(QueryNodeType::CTE_NODE) {
	}

	string ctename;
	//! The query of the CTE
	unique_ptr<QueryNode> query;
	//! Child
	unique_ptr<QueryNode> child;
	//! Aliases of the CTE node
	vector<string> aliases;

	const vector<unique_ptr<ParsedExpression>> &GetSelectList() const override {
		return query->GetSelectList();
	}

public:
	//! Convert the query node to a string
	string ToString() const override;

	bool Equals(const QueryNode *other) const override;
	//! Create a copy of this SelectNode
	unique_ptr<QueryNode> Copy() const override;

	//! Serializes a QueryNode to a stand-alone binary blob
	//! Deserializes a blob back into a QueryNode

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<QueryNode> Deserialize(Deserializer &source);
};

} // namespace duckdb
