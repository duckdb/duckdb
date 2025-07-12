#include "duckdb/planner/operator/logical_unified_string_dictionary_insertion.hpp"

namespace duckdb {

void LogicalUnifiedStringDictionaryInsertion::ResolveTypes() {
	types = children[0]->types;
}

} // namespace duckdb
