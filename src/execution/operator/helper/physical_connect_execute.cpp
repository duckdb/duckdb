#include "duckdb/execution/operator/helper/physical_connect_execute.hpp"

namespace duckdb {

SourceResultType PhysicalConnectExecute::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                         OperatorSourceInput &input) const {
	return SourceResultType::FINISHED;
}

} // namespace duckdb
