#include "duckdb/planner/operator/logical_copy_database.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"

namespace duckdb {

LogicalCopyDatabase::LogicalCopyDatabase(unique_ptr<CopyDatabaseInfo> info_p)
    : LogicalOperator(LogicalOperatorType::LOGICAL_COPY_DATABASE), info(std::move(info_p)) {
}

LogicalCopyDatabase::LogicalCopyDatabase(unique_ptr<ParseInfo> info_p)
    : LogicalOperator(LogicalOperatorType::LOGICAL_COPY_DATABASE),
      info(unique_ptr_cast<ParseInfo, CopyDatabaseInfo>(std::move(info_p))) {
}

LogicalCopyDatabase::~LogicalCopyDatabase() {
}

void LogicalCopyDatabase::ResolveTypes() {
	types.emplace_back(LogicalType::BOOLEAN);
}

} // namespace duckdb
