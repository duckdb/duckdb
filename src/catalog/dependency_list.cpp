#include "duckdb/catalog/dependency_list.hpp"
#include "duckdb/catalog/catalog_entry.hpp"

namespace duckdb {

void DependencyList::AddDependency(CatalogEntry *entry) {
	if (entry->internal) {
		return;
	}
	set.insert(entry);
}

void DependencyList::VerifyDependencies(Catalog *catalog, const string &name) {
	for(auto &dep : set) {
		if (dep->catalog != catalog) {
			throw BinderException("Error while binding \"%s\" - cannot introduce dependency with catalog entry \"%s\" in catalog \"%s\" - all dependencies must be in catalog \"%s\"", name, dep->name, dep->catalog->GetName(), catalog->GetName());
		}
	}
}


} // namespace duckdb
