#include "duckdb/catalog/catalog_entry/dependency_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/dependency_set_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/dependency_manager.hpp"

namespace duckdb {

DependencyCatalogEntry::DependencyCatalogEntry(DependencyLinkSide side, Catalog &catalog,
                                               DependencySetCatalogEntry &set, const LogicalDependency &internal,
                                               DependencyType dependency_type)
    : InCatalogEntry(CatalogType::DEPENDENCY_ENTRY, catalog, DependencyManager::MangleName(internal)),
      internal(internal), dependency_type(dependency_type), side(side), set(set) {
	D_ASSERT(EntryType() != CatalogType::DEPENDENCY_ENTRY);
	D_ASSERT(EntryType() != CatalogType::DEPENDENCY_SET);
}

const string &DependencyCatalogEntry::MangledName() const {
	return name;
}

CatalogType DependencyCatalogEntry::EntryType() const {
	return internal.type;
}

const string &DependencyCatalogEntry::EntrySchema() const {
	return internal.schema;
}

const string &DependencyCatalogEntry::EntryName() const {
	return internal.name;
}

DependencyType DependencyCatalogEntry::Type() const {
	return dependency_type;
}

const LogicalDependency &DependencyCatalogEntry::Internal() const {
	return internal;
}

DependencyCatalogEntry::~DependencyCatalogEntry() {
}

void DependencyCatalogEntry::CompleteLink(CatalogTransaction transaction, DependencyType type) {
	auto &manager = set.Manager();
	switch (side) {
	case DependencyLinkSide::DEPENDENCY: {
		auto &other_set = manager.GetOrCreateDependencySet(transaction, internal);
		other_set.AddDependent(transaction, set, type);
		break;
	}
	case DependencyLinkSide::DEPENDENT: {
		auto &other_set = manager.GetOrCreateDependencySet(transaction, internal);
		other_set.AddDependency(transaction, set, type);
		break;
	}
	}
}

DependencyCatalogEntry &DependencyCatalogEntry::GetLink(optional_ptr<CatalogTransaction> transaction) {
	auto &manager = set.Manager();
	switch (side) {
	case DependencyLinkSide::DEPENDENCY: {
		auto &other_set = *manager.GetDependencySet(transaction, internal);
		return other_set.GetDependent(transaction, set);
	}
	case DependencyLinkSide::DEPENDENT: {
		auto &other_set = *manager.GetDependencySet(transaction, internal);
		return other_set.GetDependency(transaction, set);
	}
	default:
		throw InternalException(
		    "This really shouldnt happen, there are only two parts to a link, DEPENDENCY and DEPENDENT");
	}
}

} // namespace duckdb
