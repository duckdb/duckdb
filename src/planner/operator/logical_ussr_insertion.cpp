#include "duckdb/planner/operator/logical_ussr_insertion.h"

namespace duckdb {

void LogicalUSSRInsertion::ResolveTypes() {
	types = children[0]->types;
}

} // namespace duckdb
