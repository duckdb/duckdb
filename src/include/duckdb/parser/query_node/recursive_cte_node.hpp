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

namespace duckdb {

class RecursiveCTENode : public QueryNode {
public:
	static constexpr const QueryNodeType TYPE = QueryNodeType::RECURSIVE_CTE_NODE;

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
	//! targets for key variants
	vector<unique_ptr<ParsedExpression>> key_targets;

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
