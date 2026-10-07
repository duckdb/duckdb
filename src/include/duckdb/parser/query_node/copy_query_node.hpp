//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/query_node/copy_query_node.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/query_node.hpp"

namespace duckdb {

class Deserializer;
struct CopyInfo;
struct ParseInfo;
class Serializer;

//! CopyQueryNode represents a COPY TO statement as a QueryNode,
//! enabling serialization and use as a CTE body.
class CopyQueryNode : public QueryNode {
public:
	static constexpr const QueryNodeType TYPE = QueryNodeType::COPY_QUERY_NODE;

public:
	explicit CopyQueryNode(unique_ptr<ParseInfo> info);
	~CopyQueryNode() override;

	unique_ptr<CopyInfo> info;

public:
	string ToString() const override;
	bool Equals(const QueryNode *other) const override;
	unique_ptr<QueryNode> Copy() const override;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<QueryNode> Deserialize(Deserializer &deserializer);
};

} // namespace duckdb
