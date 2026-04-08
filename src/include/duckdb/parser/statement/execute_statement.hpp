//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/statement/execute_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>

#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/parser/sql_statement.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/enums/statement_type.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/unique_ptr.hpp"

namespace duckdb {
class ParsedExpression;

class ExecuteStatement : public SQLStatement {
public:
	static constexpr const StatementType TYPE = StatementType::EXECUTE_STATEMENT;

public:
	ExecuteStatement();

	string name;
	case_insensitive_map_t<unique_ptr<ParsedExpression>> named_values;

protected:
	ExecuteStatement(const ExecuteStatement &other);

public:
	unique_ptr<SQLStatement> Copy() const override;
	string ToString() const override;
};
} // namespace duckdb
