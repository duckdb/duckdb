//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/statement/refresh_feature_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/sql_statement.hpp"

namespace duckdb {

class RefreshFeatureStatement : public SQLStatement {
public:
	static constexpr const StatementType TYPE = StatementType::REFRESH_FEATURE_STATEMENT;

public:
	RefreshFeatureStatement();

	string feature_name;
	//! Optional AT timestamp literal; when empty the feature is snapshotted at the current time.
	string at_timestamp;

protected:
	RefreshFeatureStatement(const RefreshFeatureStatement &other);

public:
	unique_ptr<SQLStatement> Copy() const override;
	string ToString() const override;
};

} // namespace duckdb
