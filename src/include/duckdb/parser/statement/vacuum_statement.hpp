//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/statement/vacuum_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/parser/sql_statement.hpp"
#include "duckdb/parser/parsed_data/vacuum_info.hpp"

namespace duckdb {

class VacuumStatement : public SQLStatement {
public:
	static constexpr const StatementType TYPE = StatementType::VACUUM_STATEMENT;

public:
	explicit VacuumStatement(const VacuumOptions &options);

	unique_ptr<VacuumInfo> info;

protected:
	VacuumStatement(const VacuumStatement &other);

public:
	unique_ptr<SQLStatement> Copy() const override;
};

} // namespace duckdb
