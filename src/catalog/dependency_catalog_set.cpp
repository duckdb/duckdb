#include "duckdb/catalog/dependency_catalog_set.hpp"
#include "duckdb/catalog/catalog_entry/dependency/dependency_entry.hpp"
#include "duckdb/catalog/dependency_list.hpp"

namespace duckdb {

MangledDependencyName DependencyCatalogSet::ApplyPrefix(const MangledEntryName &name) const {
	return MangledDependencyName(mangled_name, name);
}

bool DependencyCatalogSet::CreateEntry(CatalogTransaction transaction, const MangledEntryName &name,
                                       unique_ptr<CatalogEntry> value) {
	auto new_name = ApplyPrefix(name);
	const LogicalDependencyList EMPTY_DEPENDENCIES;
	return set.CreateEntry(transaction, new_name.name, std::move(value), EMPTY_DEPENDENCIES);
}

CatalogSet::EntryLookup DependencyCatalogSet::GetEntryDetailed(CatalogTransaction transaction,
                                                               const MangledEntryName &name) {
	auto new_name = ApplyPrefix(name);
	return set.GetEntryDetailed(transaction, new_name.name);
}

optional_ptr<CatalogEntry> DependencyCatalogSet::GetEntry(CatalogTransaction transaction,
                                                          const MangledEntryName &name) {
	auto new_name = ApplyPrefix(name);
	return set.GetEntry(transaction, new_name.name);
}

void DependencyCatalogSet::Scan(CatalogTransaction transaction, const std::function<void(CatalogEntry &)> &callback) {
	set.ScanWithPrefix(
	    transaction,
	    [&](CatalogEntry &entry) {
		    auto &dep = entry.Cast<DependencyEntry>();
		    auto &from = dep.SourceMangledName();
		    if (!StringUtil::CIEquals(from.name, mangled_name.name)) {
			    return;
		    }
		    callback(entry);
	    },
	    mangled_name.name);
}

bool DependencyCatalogSet::DropEntry(CatalogTransaction transaction, const MangledEntryName &name, bool cascade,
                                     bool allow_drop_internal) {
	auto new_name = ApplyPrefix(name);
	return set.DropEntry(transaction, new_name.name, cascade, allow_drop_internal);
}

} // namespace duckdb
