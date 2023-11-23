#include "duckdb/catalog/catalog_entry/dependency/dependency_reliant_entry.hpp"

namespace duckdb {

DependencyReliantEntry::DependencyReliantEntry(Catalog &catalog, const DependencyInfo &info)
    : DependencyEntry(catalog, DependencyEntryType::RELIANT,
                      MangledDependencyName(DependencyManager::MangleName(info.dependency.entry),
                                            DependencyManager::MangleName(info.dependent.entry)),
                      info) {
}

const MangledEntryName &DependencyReliantEntry::EntryMangledName() const {
	return dependent_name;
}

const CatalogEntryInfo &DependencyReliantEntry::EntryInfo() const {
	return dependent.entry;
}

DependencyReliantEntry::~DependencyReliantEntry() {
}

} // namespace duckdb
