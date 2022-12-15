//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/statement/attach_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/attach_info.hpp"
#include "duckdb/parser/sql_statement.hpp"

namespace duckdb {

class AttachStatement : public SQLStatement {
public:
	AttachStatement();

	unique_ptr<AttachInfo> info;

protected:
	AttachStatement(const AttachStatement &other);

public:
	unique_ptr<SQLStatement> Copy() const override;
};

} // namespace duckdb
