//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/statement/multi_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>

#include "duckdb/parser/sql_statement.hpp"
#include "duckdb/common/enums/statement_type.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

class MultiStatement : public SQLStatement {
public:
	static constexpr const StatementType TYPE = StatementType::MULTI_STATEMENT;

public:
	MultiStatement();

	vector<unique_ptr<SQLStatement>> statements;

protected:
	MultiStatement(const MultiStatement &other);

public:
	unique_ptr<SQLStatement> Copy() const override;
	string ToString() const override;
};

} // namespace duckdb
