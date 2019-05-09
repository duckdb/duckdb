//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parser/statement/select_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/unordered_map.hpp"
#include "parser/parsed_expression.hpp"
#include "parser/query_node.hpp"
#include "parser/query_node/select_node.hpp"
#include "parser/sql_statement.hpp"
#include "parser/tableref.hpp"

namespace duckdb {

//! SelectStatement is a typical SELECT clause
class SelectStatement : public SQLStatement {
public:
	SelectStatement() : SQLStatement(StatementType::SELECT) {
	}

	bool Equals(const SQLStatement *other) const;

	//! CTEs
	unordered_map<string, unique_ptr<QueryNode>> cte_map;
	//! The main query node
	unique_ptr<QueryNode> node;

	//! Create a copy of this SelectStatement
	unique_ptr<SelectStatement> Copy();
	//! Serializes a SelectStatement to a stand-alone binary blob
	void Serialize(Serializer &serializer);
	//! Deserializes a blob back into a SelectStatement, returns nullptr if
	//! deserialization is not possible
	static unique_ptr<SelectStatement> Deserialize(Deserializer &source);
};
} // namespace duckdb
