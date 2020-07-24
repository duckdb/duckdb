#include "duckdb/execution/operator/persistent/physical_copy_to_file.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

#include <algorithm>
#include <fstream>

using namespace std;

namespace duckdb {


void PhysicalCopyToFile::GetChunkInternal(ExecutionContext &context, DataChunk &chunk, PhysicalOperatorState *state) {
	auto &info = *this->info;
	idx_t total = 0;

	throw NotImplementedException("eek");

	chunk.SetCardinality(1);
	chunk.SetValue(0, 0, Value::BIGINT(total));

	state->finished = true;
}

} // namespace duckdb
