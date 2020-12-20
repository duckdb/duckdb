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
	DropStatement();

	unique_ptr<DropInfo> info;

public:
	unique_ptr<SQLStatement> Copy() const override;
};

} // namespace duckdb
