//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/statement/create_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/create_info.hpp"
#include "duckdb/parser/sql_statement.hpp"

namespace duckdb {

class CreateStatement : public SQLStatement {
public:
	CreateStatement() : SQLStatement(StatementType::CREATE_STATEMENT){};

	unique_ptr<CreateInfo> info;

public:
	unique_ptr<SQLStatement> Copy() const override {
		throw NotImplementedException("Unimplemented type for Copy");
	}
};

} // namespace duckdb
