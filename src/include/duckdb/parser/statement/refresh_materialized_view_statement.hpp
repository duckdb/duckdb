//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/statement/refresh_materialized_view_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/create_info.hpp"
#include "duckdb/parser/sql_statement.hpp"

namespace duckdb {

class RefreshMaterializedViewStatement : public SQLStatement {
public:
	static constexpr const StatementType TYPE = StatementType::REFRESH_MATERIALIZED_VIEW_STATEMENT;

public:
	RefreshMaterializedViewStatement();

	unique_ptr<CreateInfo> info;

protected:
	RefreshMaterializedViewStatement(const RefreshMaterializedViewStatement &other);

public:
	unique_ptr<SQLStatement> Copy() const override;
	string ToString() const override;
};

} // namespace duckdb
