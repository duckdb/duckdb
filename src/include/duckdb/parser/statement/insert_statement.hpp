//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/statement/insert_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/query_node.hpp"
#include "duckdb/parser/statement/update_statement.hpp"
#include "duckdb/parser/query_node/insert_query_node.hpp"

namespace duckdb {
class ExpressionListRef;
class UpdateSetInfo;
class Serializer;
class Deserializer;

enum class OnConflictAction : uint8_t {
	THROW,
	NOTHING,
	UPDATE,
	REPLACE // Only used in transform/bind step, changed to UPDATE later
};

enum class InsertColumnOrder : uint8_t { INSERT_BY_POSITION = 0, INSERT_BY_NAME = 1 };

class OnConflictInfo {
public:
	OnConflictInfo();

public:
	unique_ptr<OnConflictInfo> Copy() const;
	static bool Equals(const unique_ptr<OnConflictInfo> &left, const unique_ptr<OnConflictInfo> &right);

	void Serialize(Serializer &serializer) const;
	static unique_ptr<OnConflictInfo> Deserialize(Deserializer &deserializer);

	static string ActionToString(OnConflictAction action);

public:
	OnConflictAction action_type;

	vector<string> indexed_columns;
	//! The SET information (if action_type == UPDATE)
	unique_ptr<UpdateSetInfo> set_info;
	//! The condition determining whether we apply the DO .. for conflicts that arise
	unique_ptr<ParsedExpression> condition;

protected:
	OnConflictInfo(const OnConflictInfo &other);
};

class InsertStatement : public SQLStatement {
public:
	static constexpr const StatementType TYPE = StatementType::INSERT_STATEMENT;

public:
	InsertStatement();

	unique_ptr<InsertQueryNode> node;

protected:
	InsertStatement(const InsertStatement &other);

public:
	string ToString() const override;
	unique_ptr<SQLStatement> Copy() const override;

	//! If the INSERT statement is inserted DIRECTLY from a values list (i.e. INSERT INTO tbl VALUES (...)) this returns
	//! the expression list Otherwise, this returns NULL
	optional_ptr<ExpressionListRef> GetValuesList() const;
};

} // namespace duckdb
