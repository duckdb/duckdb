#include "duckdb/execution/operator/scan/physical_empty_result.hpp"

namespace duckdb {

SourceResultType PhysicalEmptyResult::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                      OperatorSourceInput &input) const {
	return SourceResultType::FINISHED;
}

} // namespace duckdb
