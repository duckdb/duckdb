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
	//! Constructor for non-owning reference (common path)
	explicit StatementNode(SQLStatement &stmt_p);
	//! Constructor for owning the statement (for DML in CTEs)
	explicit StatementNode(unique_ptr<SQLStatement> owned_stmt_p);

	SQLStatement &stmt;
	//! If set, this StatementNode owns the underlying statement
	unique_ptr<SQLStatement> owned_statement;

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
