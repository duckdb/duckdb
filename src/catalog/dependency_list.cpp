#include "duckdb/catalog/dependency_list.hpp"

namespace duckdb {

void DependencyList::AddDependency(CatalogEntry *entry) {
	if (entry->internal) {
		return;
	}
	set.insert(entry);
}

} // namespace duckdb
