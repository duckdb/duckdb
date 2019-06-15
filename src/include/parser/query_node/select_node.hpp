//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parser/query_node/select_node.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/parsed_expression.hpp"
#include "parser/query_node.hpp"
#include "parser/sql_statement.hpp"
#include "parser/tableref.hpp"

namespace duckdb {

//! SelectNode represents a standard SELECT statement
class SelectNode : public QueryNode {
public:
	SelectNode() : QueryNode(QueryNodeType::SELECT_NODE) {
	}

	//! The projection list
	vector<unique_ptr<ParsedExpression>> select_list;
	//! The FROM clause
	unique_ptr<TableRef> from_table;
	//! The WHERE clause
	unique_ptr<ParsedExpression> where_clause;
	//! list of groups
	vector<unique_ptr<ParsedExpression>> groups;
	//! list of distinct on targets
	vector<unique_ptr<ParsedExpression>> distinct_on_targets;
	//! HAVING clause
	unique_ptr<ParsedExpression> having;
	//! Value list, only used for VALUES statement
	vector<vector<unique_ptr<ParsedExpression>>> values;

	const vector<unique_ptr<ParsedExpression>> &GetSelectList() const override {
		return select_list;
	}

public:
	// //! Whether or not the query has a GROUP BY clause
	// bool HasGroup() {
	// 	return groups.size() > 0;
	// }
	// //! Whether or not the query has a HAVING clause
	// bool HasHaving() {
	// 	return having.get();
	// }
	// //! Whether or not the query has an AGGREGATION
	// bool HasAggregation() {
	// 	return HasGroup();
	// }

	bool Equals(const QueryNode *other) const override;
	//! Create a copy of this SelectNode
	unique_ptr<QueryNode> Copy() override;
	//! Serializes a SelectNode to a stand-alone binary blob
	void Serialize(Serializer &serializer) override;
	//! Deserializes a blob back into a SelectNode
	static unique_ptr<QueryNode> Deserialize(Deserializer &source);
};
}; // namespace duckdb
