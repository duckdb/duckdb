//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/statement/show_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/sql_statement.hpp"
#include "duckdb/parser/parsed_data/show_select_info.hpp"

namespace duckdb {

class ShowStatement : public SQLStatement {
public:
	ShowStatement() : SQLStatement(StatementType::SHOW_STATEMENT), info(make_unique<ShowSelectInfo>()){};
	//ShowStatement() : SQLStatement(StatementType::SHOW_STATEMENT), info(make_unique<ShowSelectInfo>()){};

	unique_ptr<ShowSelectInfo> info;
};

} // namespace duckdb
