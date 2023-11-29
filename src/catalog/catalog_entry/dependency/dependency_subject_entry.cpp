#include "duckdb/catalog/catalog_entry/dependency/dependency_subject_entry.hpp"

namespace duckdb {

DependencySubjectEntry::DependencySubjectEntry(Catalog &catalog, const DependencyInfo &info)
    : DependencyEntry(catalog, DependencyEntryType::SUBJECT,
                      MangledDependencyName(DependencyManager::MangleName(info.dependent.entry),
                                            DependencyManager::MangleName(info.subject.entry)),
                      info) {
}

const MangledEntryName &DependencySubjectEntry::EntryMangledName() const {
	return subject_name;
}

const CatalogEntryInfo &DependencySubjectEntry::EntryInfo() const {
	return subject.entry;
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
