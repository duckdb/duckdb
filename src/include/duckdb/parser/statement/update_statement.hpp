//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/statement/update_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/parser/sql_statement.hpp"
#include "duckdb/parser/tableref.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/parser/query_node.hpp"

namespace duckdb {

class Serializer;
class Deserializer;

class UpdateSetInfo {
public:
	UpdateSetInfo();

public:
	unique_ptr<UpdateSetInfo> Copy() const;
	string ToString() const;
	static bool Equals(const unique_ptr<UpdateSetInfo> &left, const unique_ptr<UpdateSetInfo> &right);

	void Serialize(Serializer &serializer) const;
	static unique_ptr<UpdateSetInfo> Deserialize(Deserializer &deserializer);

public:
	// The condition that needs to be met to perform the update
	unique_ptr<ParsedExpression> condition;
	// The columns to update
	vector<string> columns;
	// The set expressions to execute
	vector<unique_ptr<ParsedExpression>> expressions;

protected:
	UpdateSetInfo(const UpdateSetInfo &other);
};

class UpdateQueryNode;

class UpdateStatement : public SQLStatement {
public:
	static constexpr const StatementType TYPE = StatementType::UPDATE_STATEMENT;

public:
	UpdateStatement();

	unique_ptr<UpdateQueryNode> node;

protected:
	UpdateStatement(const UpdateStatement &other);

public:
	string ToString() const override;
	unique_ptr<SQLStatement> Copy() const override;
};

} // namespace duckdb
