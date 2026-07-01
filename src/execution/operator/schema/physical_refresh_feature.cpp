#include "duckdb/execution/operator/schema/physical_refresh_feature.hpp"

#include "duckdb/common/feature_refresh.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

SourceResultType PhysicalRefreshFeature::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                         OperatorSourceInput &input) const {
	auto result = RefreshFeature(context.client, feature_name);
	chunk.SetCardinality(1);
	chunk.data[0].Append(Value::BIGINT(NumericCast<int64_t>(result.rows_affected)));
	return SourceResultType::FINISHED;
}

} // namespace duckdb