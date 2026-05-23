#include "duckdb/execution/operator/schema/physical_create_feature.hpp"
#include "duckdb/common/exception.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
SourceResultType PhysicalCreateFeature::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                        OperatorSourceInput &input) const {
	// For now, just report that we parsed the feature successfully
	// TODO: implement actual feature materialization
	throw NotImplementedException("CREATE FEATURE is parsed but execution is not yet implemented. "
	                              "Feature: %s, Source: %s, Entity: %s, Timestamp: %s",
	                              info->feature_name, info->source_table, info->entity_column,
	                              info->timestamp_column);
}

} // namespace duckdb
