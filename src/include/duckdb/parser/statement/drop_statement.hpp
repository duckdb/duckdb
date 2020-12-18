//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/statement/drop_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/parser/sql_statement.hpp"

namespace duckdb {

class DropStatement : public SQLStatement {
public:
	DropStatement() : SQLStatement(StatementType::DROP_STATEMENT), info(make_unique<DropInfo>()){};

	unique_ptr<DropInfo> info;

public:
	unique_ptr<SQLStatement> Copy() const override {
		throw NotImplementedException("Unimplemented type for Copy");
	}
};

} // namespace duckdb
