//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/statement/set_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/enums/set_scope.h"
#include "duckdb/parser/sql_statement.hpp"
#include "duckdb/common/types/value.hpp"

namespace duckdb {

class SetStatement : public SQLStatement {
public:
	SetStatement(std::string name_p, Value value_p, SetScope scope_p);

public:
	unique_ptr<SQLStatement> Copy() const override;

	std::string name;
	Value value;
	SetScope scope;
};
} // namespace duckdb
