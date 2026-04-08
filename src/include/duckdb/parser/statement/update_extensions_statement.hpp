//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/statement/update_extensions_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>

#include "duckdb/parser/sql_statement.hpp"
#include "duckdb/parser/parsed_data/update_extensions_info.hpp"
#include "duckdb/common/enums/statement_type.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/unique_ptr.hpp"

namespace duckdb {

class UpdateExtensionsStatement : public SQLStatement {
public:
	static constexpr const StatementType TYPE = StatementType::UPDATE_EXTENSIONS_STATEMENT;

public:
	UpdateExtensionsStatement();
	unique_ptr<UpdateExtensionsInfo> info;

protected:
	UpdateExtensionsStatement(const UpdateExtensionsStatement &other);

public:
	string ToString() const override;
	unique_ptr<SQLStatement> Copy() const override;
};

} // namespace duckdb
