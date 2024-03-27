#include "duckdb/planner/operator/logical_copy_database.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"

namespace duckdb {

LogicalCopyDatabase::LogicalCopyDatabase(unique_ptr<CopyDatabaseInfo> info_p)
    : LogicalOperator(LogicalOperatorType::LOGICAL_COPY_DATABASE), info(std::move(info_p)) {
}

void LogicalCopyDatabase::Serialize(Serializer &serializer) const {
	throw NotImplementedException("LogicalCopyDatabase::Serialize");
}
unique_ptr<LogicalOperator> LogicalCopyDatabase::Deserialize(Deserializer &deserializer) {
	throw NotImplementedException("LogicalCopyDatabase::Deserialize");
}

void LogicalCopyDatabase::ResolveTypes() {
	types.emplace_back(LogicalType::BOOLEAN);
}

} // namespace duckdb
