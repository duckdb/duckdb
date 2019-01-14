//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parser/statement/create_view_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/parsed_data.hpp"
#include "parser/sql_statement.hpp"

namespace duckdb {

class CreateViewStatement : public SQLStatement {
public:
	CreateViewStatement() : SQLStatement(StatementType::CREATE_VIEW), info(make_unique<CreateViewInformation>()){};

	string ToString() const override {
		return "CREATE VIEW";
	}

	unique_ptr<CreateViewInformation> info;
};

} // namespace duckdb
//
