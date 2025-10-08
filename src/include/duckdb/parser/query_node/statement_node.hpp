//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/query_node/statement_node.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/parser/query_node.hpp"
#include "duckdb/parser/sql_statement.hpp"

namespace duckdb {

class StatementNode : public QueryNode {
public:
	static constexpr const QueryNodeType TYPE = QueryNodeType::STATEMENT_NODE;

public:
	explicit StatementNode(SQLStatement &stmt_p);

	SQLStatement &stmt;

public:
	const vector<unique_ptr<ParsedExpression>> &GetSelectList() const override;
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
