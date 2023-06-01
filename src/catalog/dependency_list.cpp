#include "duckdb/catalog/dependency_list.hpp"
#include "duckdb/catalog/catalog_entry.hpp"
#include "duckdb/catalog/catalog.hpp"

namespace duckdb {

void DependencyList::AddDependency(CatalogEntry *entry) {
	if (entry->internal) {
		return;
	}
	set.insert(entry);
}

void DependencyList::VerifyDependencies(Catalog *catalog, const string &name) {
	for (auto &dep : set) {
		if (dep->catalog != catalog) {
			throw DependencyException(
			    "Error adding dependency for object \"%s\" - dependency \"%s\" is in catalog "
			    "\"%s\", which does not match the catalog \"%s\".\nCross catalog dependencies are not supported.",
			    name, dep->name, dep->catalog->GetName(), catalog->GetName());
		}
	}
}

} // namespace duckdb
