//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/statement/detach_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>

#include "duckdb/parser/parsed_data/detach_info.hpp"
#include "duckdb/parser/sql_statement.hpp"
#include "duckdb/common/enums/statement_type.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/unique_ptr.hpp"

namespace duckdb {

class DetachStatement : public SQLStatement {
public:
	static constexpr const StatementType TYPE = StatementType::DETACH_STATEMENT;

public:
	DetachStatement();

	unique_ptr<DetachInfo> info;

protected:
	DetachStatement(const DetachStatement &other);

public:
	unique_ptr<SQLStatement> Copy() const override;
	string ToString() const override;
};

} // namespace duckdb
