//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/statement/create_secret_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/create_info.hpp"
#include "duckdb/parser/parsed_data/create_secret_info.hpp"
#include "duckdb/parser/sql_statement.hpp"

namespace duckdb {

class CreateSecretStatement : public SQLStatement {
public:
	static constexpr const StatementType TYPE = StatementType::CREATE_SECRET_STATEMENT;

public:
	CreateSecretStatement(string type, OnCreateConflict on_conflict);

	unique_ptr<CreateSecretInfo> info;

protected:
	CreateSecretStatement(const CreateSecretStatement &other);

public:
	unique_ptr<SQLStatement> Copy() const override;
};

} // namespace duckdb
