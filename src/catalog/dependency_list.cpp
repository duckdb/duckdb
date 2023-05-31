#include "duckdb/catalog/dependency_list.hpp"
#include "duckdb/catalog/catalog_entry.hpp"
#include "duckdb/catalog/catalog.hpp"

namespace duckdb {

void DependencyList::AddDependency(CatalogEntry &entry) {
	if (entry.internal) {
		return;
	}
	set.insert(entry);
}

void DependencyList::VerifyDependencies(Catalog &catalog, const string &name) {
	for (auto &dep_entry : set) {
		auto &dep = dep_entry.get();
		if (&dep.ParentCatalog() != &catalog) {
			throw DependencyException(
			    "Error adding dependency for object \"%s\" - dependency \"%s\" is in catalog "
			    "\"%s\", which does not match the catalog \"%s\".\nCross catalog dependencies are not supported.",
			    name, dep.name, dep.ParentCatalog().GetName(), catalog.GetName());
		}
	}
}

} // namespace duckdb
