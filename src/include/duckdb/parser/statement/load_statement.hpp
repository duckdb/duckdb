//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/statement/set_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/sql_statement.hpp"

namespace duckdb {

class LoadStatement : public SQLStatement {
public:
	LoadStatement(std::string file_p);

public:
	unique_ptr<SQLStatement> Copy() const override;

	std::string file;
};
} // namespace duckdb
