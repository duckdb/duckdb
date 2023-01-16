#include "duckdb/catalog/dependency_list.hpp"
#include "duckdb/catalog/catalog_entry.hpp"

namespace duckdb {

void DependencyList::AddDependency(CatalogEntry *entry) {
	if (entry->internal) {
		return;
	}
	set.insert(entry);
}

} // namespace duckdb
