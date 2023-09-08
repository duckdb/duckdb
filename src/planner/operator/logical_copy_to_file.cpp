#include "duckdb/planner/operator/logical_copy_to_file.hpp"

#include "duckdb/catalog/catalog_entry/copy_function_catalog_entry.hpp"
#include "duckdb/function/copy_function.hpp"

namespace duckdb {

void LogicalCopyToFile::Serialize(Serializer &serializer) const {
	throw SerializationException("LogicalCopyToFile not implemented yet");
}

unique_ptr<LogicalOperator> LogicalCopyToFile::Deserialize(Deserializer &deserializer) {
	throw SerializationException("LogicalCopyToFile not implemented yet");
}

idx_t LogicalCopyToFile::EstimateCardinality(ClientContext &context) {
	return 1;
}

} // namespace duckdb
