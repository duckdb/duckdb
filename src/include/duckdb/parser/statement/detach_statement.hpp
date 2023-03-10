//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/statement/detach_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/detach_info.hpp"
#include "duckdb/parser/sql_statement.hpp"

namespace duckdb {

class DetachStatement : public SQLStatement {
public:
	DetachStatement();

	unique_ptr<DetachInfo> info;

protected:
	DetachStatement(const DetachStatement &other);

public:
	unique_ptr<SQLStatement> Copy() const override;
};

} // namespace duckdb
