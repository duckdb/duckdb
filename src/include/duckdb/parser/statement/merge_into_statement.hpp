//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/statement/merge_into_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/parser/sql_statement.hpp"
#include "duckdb/parser/tableref.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/parser/query_node.hpp"
#include "duckdb/common/enums/merge_action_type.hpp"
#include "duckdb/parser/statement/insert_statement.hpp"

namespace duckdb {

class MergeIntoAction {
public:
	//! The merge action type
	MergeActionType action_type;
	//! Condition - or NULL if this should always be performed for the given action
	unique_ptr<ParsedExpression> condition;
	//! The SET information (if action_type == MERGE_UPDATE)
	unique_ptr<UpdateSetInfo> update_info;
	//! Column names to insert into (if action_type == MERGE_INSERT)
	vector<string> insert_columns;
	//! Set of expressions for INSERT
	vector<unique_ptr<ParsedExpression>> expressions;
	//! INSERT BY POSITION or INSERT BY NAME
	InsertColumnOrder column_order = InsertColumnOrder::INSERT_BY_POSITION;
	//! Whether or not this is a INSERT DEFAULT VALUES
	bool default_values = false;

	string ToString() const;
	unique_ptr<MergeIntoAction> Copy() const;
};

class MergeIntoStatement : public SQLStatement {
public:
	static constexpr const StatementType TYPE = StatementType::MERGE_INTO_STATEMENT;

public:
	MergeIntoStatement();

	unique_ptr<TableRef> target;
	unique_ptr<TableRef> source;
	unique_ptr<ParsedExpression> join_condition;
	vector<string> using_columns;

	map<MergeActionCondition, vector<unique_ptr<MergeIntoAction>>> actions;

	//! CTEs
	CommonTableExpressionMap cte_map;

protected:
	MergeIntoStatement(const MergeIntoStatement &other);

public:
	string ToString() const override;
	unique_ptr<SQLStatement> Copy() const override;

	static string ActionConditionToString(MergeActionCondition condition);
};

} // namespace duckdb
