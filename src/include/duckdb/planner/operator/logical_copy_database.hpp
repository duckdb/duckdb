//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_copy_database.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/copy_info.hpp"
#include "duckdb/parser/parsed_data/exported_table_data.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/function/copy_function.hpp"

namespace duckdb {

struct CopyDatabaseInfo {
	CopyDatabaseInfo(Catalog &from_database, Catalog &to_database);
	~CopyDatabaseInfo();

	Catalog &from_database;
	Catalog &to_database;
	vector<unique_ptr<CreateInfo>> entries;
};

class LogicalCopyDatabase : public LogicalOperator {
public:
	static constexpr const LogicalOperatorType TYPE = LogicalOperatorType::LOGICAL_COPY_DATABASE;

public:
	explicit LogicalCopyDatabase(unique_ptr<CopyDatabaseInfo> info_p);
	~LogicalCopyDatabase() override;

	unique_ptr<CopyDatabaseInfo> info;

public:
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<LogicalOperator> Deserialize(Deserializer &deserializer);

protected:
	void ResolveTypes() override;
};

} // namespace duckdb
