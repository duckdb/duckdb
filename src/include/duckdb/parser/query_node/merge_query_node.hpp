//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/query_node/merge_query_node.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/identifier.hpp"
#include "duckdb/parser/query_node.hpp"
#include "duckdb/parser/tableref.hpp"
#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/common/map.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/enums/merge_action_type.hpp"

namespace duckdb {
class Serializer;
class Deserializer;
class MergeIntoAction;

//! MergeQueryNode represents a MERGE INTO statement
class MergeQueryNode : public QueryNode {
public:
	static constexpr const QueryNodeType TYPE = QueryNodeType::MERGE_QUERY_NODE;

public:
	MergeQueryNode();

	//! The table to merge into
	unique_ptr<TableRef> target;
	//! The source of the merge
	unique_ptr<TableRef> source;
	//! The join condition (ON clause) - or NULL if a USING column list is used
	unique_ptr<ParsedExpression> join_condition;
	//! The columns used in the USING clause (if join_condition is NULL)
	vector<Identifier> using_columns;
	//! The merge actions, grouped by their condition
	map<MergeActionCondition, vector<unique_ptr<MergeIntoAction>>> actions;
	//! keep track of optional returningList if statement contains a RETURNING keyword
	vector<unique_ptr<ParsedExpression>> returning_list;

public:
	string ToString() const override;
	bool Equals(const QueryNode *other) const override;
	unique_ptr<QueryNode> Copy() const override;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<QueryNode> Deserialize(Deserializer &deserializer);

	static string ActionConditionToString(MergeActionCondition condition);
};

} // namespace duckdb
