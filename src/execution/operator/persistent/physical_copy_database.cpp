#include "duckdb/execution/operator/persistent/physical_copy_database.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
SourceResultType PhysicalCopyDatabase::GetData(ExecutionContext &context, DataChunk &chunk,
                                         OperatorSourceInput &input) const {
	return SourceResultType::FINISHED;
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
SinkResultType PhysicalCopyDatabase::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	// nop
	return SinkResultType::NEED_MORE_INPUT;
}

vector<const_reference<PhysicalOperator>> PhysicalCopyDatabase::GetSources() const {
	return {*this};
}

} // namespace duckdb
