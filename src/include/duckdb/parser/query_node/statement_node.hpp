//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/query_node/statement_node.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>

#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/parser/query_node.hpp"
#include "duckdb/parser/sql_statement.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/unique_ptr.hpp"

namespace duckdb {
class Deserializer;
class SQLStatement;
class Serializer;

class StatementNode : public QueryNode {
public:
	static constexpr const QueryNodeType TYPE = QueryNodeType::STATEMENT_NODE;

public:
	explicit StatementNode(SQLStatement &stmt_p);

	SQLStatement &stmt;

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
