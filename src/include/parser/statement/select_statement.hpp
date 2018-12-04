//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parser/statement/select_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/expression.hpp"
#include "parser/sql_node_visitor.hpp"
#include "parser/sql_statement.hpp"
#include "parser/tableref.hpp"
#include "parser/query_node.hpp"
#include "parser/query_node/select_node.hpp"

namespace duckdb {

//! SelectStatement is a typical SELECT clause
class SelectStatement : public SQLStatement {
public:
	SelectStatement() : SQLStatement(StatementType::SELECT) {
	}

	virtual std::string ToString() const;
	virtual std::unique_ptr<SQLStatement> Accept(SQLNodeVisitor *v) {
		return v->Visit(*this);
	}

	virtual bool Equals(const SQLStatement *other);

	//! CTEs
	std::map<std::string, std::unique_ptr<QueryNode>> cte_map;

	//! The main query node
	std::unique_ptr<QueryNode> node;

	//! Create a copy of this SelectStatement
	std::unique_ptr<SelectStatement> Copy();
	//! Serializes a SelectStatement to a stand-alone binary blob
	void Serialize(Serializer &serializer);
	//! Deserializes a blob back into a SelectStatement, returns nullptr if
	//! deserialization is not possible
	static std::unique_ptr<SelectStatement> Deserialize(Deserializer &source);
};
} // namespace duckdb
