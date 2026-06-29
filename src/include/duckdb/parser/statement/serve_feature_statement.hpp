//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/statement/serve_feature_statement.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/feature_serve.hpp"
#include "duckdb/parser/sql_statement.hpp"

namespace duckdb {

class ServeFeatureStatement : public SQLStatement {
public:
	static constexpr const StatementType TYPE = StatementType::SERVE_FEATURE_STATEMENT;

public:
	ServeFeatureStatement();

	vector<string> feature_names;
	vector<vector<FeatureServeEntityMapping>> entity_mappings;
	string spine_table;
	string entity_column;
	string as_of_column;

protected:
	ServeFeatureStatement(const ServeFeatureStatement &other);

public:
	unique_ptr<SQLStatement> Copy() const override;
	string ToString() const override;
};

} // namespace duckdb
