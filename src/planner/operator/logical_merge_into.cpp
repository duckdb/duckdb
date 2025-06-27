#include "duckdb/planner/operator/logical_merge_into.hpp"

namespace duckdb {

LogicalMergeInto::LogicalMergeInto(TableCatalogEntry &table)
    : LogicalOperator(LogicalOperatorType::LOGICAL_MERGE_INTO), table(table) {
}

void LogicalMergeInto::Serialize(Serializer &serializer) const {
	throw NotImplementedException("FIXME: DeSerialize");
}

unique_ptr<LogicalOperator> LogicalMergeInto::Deserialize(Deserializer &deserializer) {
	throw NotImplementedException("FIXME: DeSerialize");
}

idx_t LogicalMergeInto::EstimateCardinality(ClientContext &context) {
	return 1;
}

vector<ColumnBinding> LogicalMergeInto::GetColumnBindings() {
	return {ColumnBinding(0, 0)};
}

void LogicalMergeInto::ResolveTypes() {
	types.emplace_back(LogicalType::BIGINT);
}

} // namespace duckdb
