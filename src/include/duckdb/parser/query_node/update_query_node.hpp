//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/query_node/update_query_node.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>

#include "duckdb/parser/query_node.hpp"
#include "duckdb/parser/tableref.hpp"
#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/parser/statement/update_statement.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {
class Serializer;
class Deserializer;

//! UpdateQueryNode represents an UPDATE DML statement as a QueryNode,
//! enabling serialization and use as a CTE body.
class UpdateQueryNode : public QueryNode {
public:
	static constexpr const QueryNodeType TYPE = QueryNodeType::UPDATE_QUERY_NODE;

public:
	UpdateQueryNode();

	//! The table to update
	unique_ptr<TableRef> table;
	//! Optional FROM clause
	unique_ptr<TableRef> from_table;
	//! keep track of optional returningList if statement contains a RETURNING keyword
	vector<unique_ptr<ParsedExpression>> returning_list;
	//! The SET clause
	unique_ptr<UpdateSetInfo> set_info;
	//! bind the same way as ALTER TABLE expressions (table catalog preferred)
	bool prioritize_table_when_binding = false;

public:
	string ToString() const override;
	bool Equals(const QueryNode *other) const override;
	unique_ptr<QueryNode> Copy() const override;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<QueryNode> Deserialize(Deserializer &deserializer);
};

} // namespace duckdb
