#include "duckdb/catalog/catalog_entry/dependency/dependency_subject_entry.hpp"

namespace duckdb {

DependencySubjectEntry::DependencySubjectEntry(Catalog &catalog, const DependencyInfo &info)
    : DependencyEntry(catalog, DependencyEntryType::SUBJECT,
                      MangledDependencyName(DependencyManager::MangleName(info.dependent.entry),
                                            DependencyManager::MangleName(info.dependency.entry)),
                      info) {
}

const MangledEntryName &DependencySubjectEntry::EntryMangledName() const {
	return dependency_name;
}

const CatalogEntryInfo &DependencySubjectEntry::EntryInfo() const {
	return dependency.entry;
}

const MangledEntryName &DependencySubjectEntry::SourceMangledName() const {
	return dependent_name;
}

const CatalogEntryInfo &DependencySubjectEntry::SourceInfo() const {
	return dependent.entry;
}

DependencySubjectEntry::~DependencySubjectEntry() {
}

} // namespace duckdb
