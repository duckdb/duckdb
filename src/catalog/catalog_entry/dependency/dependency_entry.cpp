#include "duckdb/catalog/catalog_entry/dependency/dependency_entry.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/dependency_manager.hpp"
#include "duckdb/catalog/catalog.hpp"

namespace duckdb {

DependencyEntry::DependencyEntry(Catalog &catalog, DependencyEntryType side, const MangledDependencyName &name,
                                 const DependencyInfo &info)
    : InCatalogEntry(CatalogType::DEPENDENCY_ENTRY, catalog, name.name),
      dependent_name(DependencyManager::MangleName(info.dependent.entry)),
      dependency_name(DependencyManager::MangleName(info.dependency.entry)), dependent(info.dependent),
      dependency(info.dependency), side(side) {
	D_ASSERT(info.dependent.entry.type != CatalogType::DEPENDENCY_ENTRY);
	D_ASSERT(info.dependency.entry.type != CatalogType::DEPENDENCY_ENTRY);
	if (catalog.IsTemporaryCatalog()) {
		temporary = true;
	}
}

const MangledEntryName &DependencyEntry::DependencyMangledName() const {
	return dependency_name;
}

const DependencySubject &DependencyEntry::Subject() const {
	return dependency;
}

const MangledEntryName &DependencyEntry::DependentMangledName() const {
	return dependent_name;
}

const DependencyDependent &DependencyEntry::Dependent() const {
	return dependent;
}

DependencyEntry::~DependencyEntry() {
}

DependencyEntryType DependencyEntry::Side() const {
	return side;
}

} // namespace duckdb
