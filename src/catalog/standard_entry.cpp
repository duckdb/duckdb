#include "duckdb/catalog/standard_entry.hpp"
#include "duckdb/catalog/dependency_list.hpp"

namespace duckdb {

DependencyList StandardEntry::InherentDependencies() {
	return DependencyList();
}

} // namespace duckdb
