//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/query_node/delete_query_node.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/query_node.hpp"
#include "duckdb/parser/tableref.hpp"
#include "duckdb/parser/parsed_expression.hpp"

namespace duckdb {

//! DeleteQueryNode represents a DELETE DML statement as a QueryNode,
//! enabling serialization and use as a CTE body.
class DeleteQueryNode : public QueryNode {
public:
	static constexpr const QueryNodeType TYPE = QueryNodeType::DELETE_QUERY_NODE;

public:
	DeleteQueryNode();
	explicit DeleteQueryNode(DeleteStatement &stmt);

	//! WHERE condition
	unique_ptr<ParsedExpression> condition;
	//! The table to delete from
	unique_ptr<TableRef> table;
	//! USING clauses
	vector<unique_ptr<TableRef>> using_clauses;
	//! keep track of optional returningList if statement contains a RETURNING keyword
	vector<unique_ptr<ParsedExpression>> returning_list;

public:
	string ToString() const override;
	bool Equals(const QueryNode *other) const override;
	unique_ptr<QueryNode> Copy() const override;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<QueryNode> Deserialize(Deserializer &deserializer);
};

} // namespace duckdb
