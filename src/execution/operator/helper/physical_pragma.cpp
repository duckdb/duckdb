#include "duckdb/execution/operator/helper/physical_pragma.hpp"

using namespace duckdb;
using namespace std;

void PhysicalPragma::GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state) {
	throw NotImplementedException("FIXME: handle pragmas here!");
}
