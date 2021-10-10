#include "duckdb/execution/operator/scan/physical_empty_result.hpp"

namespace duckdb {

void PhysicalEmptyResult::GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate,
                                  LocalSourceState &lstate) const {
}

} // namespace duckdb
