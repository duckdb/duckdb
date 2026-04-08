//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/statement/create_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>

#include "duckdb/parser/parsed_data/create_info.hpp"
#include "duckdb/parser/sql_statement.hpp"
#include "duckdb/common/enums/statement_type.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/unique_ptr.hpp"

namespace duckdb {

class CreateStatement : public SQLStatement {
public:
	static constexpr const StatementType TYPE = StatementType::CREATE_STATEMENT;

public:
	CreateStatement();

	unique_ptr<CreateInfo> info;

protected:
	CreateStatement(const CreateStatement &other);

public:
	unique_ptr<SQLStatement> Copy() const override;
	string ToString() const override;
};

} // namespace duckdb
