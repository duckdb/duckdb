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

namespace duckdb {

//! DEPRECATED - CTENode is only preserved for backwards compatibility when serializing older databases
class CTENode : public QueryNode {
public:
	static constexpr const QueryNodeType TYPE = QueryNodeType::CTE_NODE;

public:
	CTENode() : QueryNode(QueryNodeType::CTE_NODE) {
	}

	string ctename;
	unique_ptr<QueryNode> query;
	unique_ptr<QueryNode> child;
	vector<string> aliases;

	CTEMaterialize materialized = CTEMaterialize::CTE_MATERIALIZE_DEFAULT;

public:
	string ToString() const override;

	bool Equals(const QueryNode *other) const override;
	unique_ptr<QueryNode> Copy() const override;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<QueryNode> Deserialize(Deserializer &source);
};

} // namespace duckdb
