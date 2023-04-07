//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/statement/set_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/enums/set_scope.hpp"
#include "duckdb/common/enums/set_type.hpp"
#include "duckdb/parser/sql_statement.hpp"
#include "duckdb/common/types/value.hpp"

namespace duckdb {

class SetStatement : public SQLStatement {
public:
	static constexpr const StatementType TYPE = StatementType::SET_STATEMENT;

protected:
	SetStatement(std::string name_p, SetScope scope_p, SetType type_p);
	SetStatement(const SetStatement &other) = default;

public:
	unique_ptr<SQLStatement> Copy() const override;

public:
	std::string name;
	SetScope scope;
	SetType set_type;
};

class SetVariableStatement : public SetStatement {
public:
	SetVariableStatement(std::string name_p, Value value_p, SetScope scope_p);

protected:
	SetVariableStatement(const SetVariableStatement &other) = default;

public:
	unique_ptr<SQLStatement> Copy() const override;

public:
	Value value;
};

class ResetVariableStatement : public SetStatement {
public:
	ResetVariableStatement(std::string name_p, SetScope scope_p);

protected:
	ResetVariableStatement(const ResetVariableStatement &other) = default;
};

} // namespace duckdb
