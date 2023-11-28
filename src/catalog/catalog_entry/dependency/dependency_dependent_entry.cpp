#include "duckdb/catalog/catalog_entry/dependency/dependency_dependent_entry.hpp"

namespace duckdb {

DependencyDependentEntry::DependencyDependentEntry(Catalog &catalog, const DependencyInfo &info)
    : DependencyEntry(catalog, DependencyEntryType::DEPENDENT,
                      MangledDependencyName(DependencyManager::MangleName(info.dependency.entry),
                                            DependencyManager::MangleName(info.dependent.entry)),
                      info) {
}

const MangledEntryName &DependencyDependentEntry::EntryMangledName() const {
	return dependent_name;
}

const CatalogEntryInfo &DependencyDependentEntry::EntryInfo() const {
	return dependent.entry;
}

const MangledEntryName &DependencyDependentEntry::SourceMangledName() const {
	return dependency_name;
}

const CatalogEntryInfo &DependencyDependentEntry::SourceInfo() const {
	return dependency.entry;
}

DependencyDependentEntry::~DependencyDependentEntry() {
}

} // namespace duckdb
