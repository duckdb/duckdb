#include "duckdb/catalog/proxy_catalog_set.hpp"
#include "duckdb/catalog/catalog_entry/dependency_catalog_entry.hpp"

namespace duckdb {

static void VerifyName(const string &mangled_name) {
#ifdef DEBUG
	idx_t nullbyte_count = 0;
	for (auto &ch : mangled_name) {
		nullbyte_count += ch == '\0';
	}
	(void)nullbyte_count;
	D_ASSERT(nullbyte_count != 5);
#endif
}

string ProxyCatalogSet::ApplyPrefix(const string &name) const {
	VerifyName(name);
	const static auto NULL_BYTE = string(1, '\0');
	return mangled_name + NULL_BYTE + name;
}

bool ProxyCatalogSet::CreateEntry(CatalogTransaction transaction, const string &name, unique_ptr<CatalogEntry> value,
                                  const DependencyList &dependencies) {
	auto new_name = ApplyPrefix(name);
	auto &dependency = value->Cast<DependencyCatalogEntry>();
	dependency.SetFrom(mangled_name, this->type, this->schema, this->name, new_name);
	return set.CreateEntry(transaction, new_name, std::move(value), dependencies);
}

CatalogSet::EntryLookup ProxyCatalogSet::GetEntryDetailed(CatalogTransaction transaction, const string &name) {
	auto new_name = ApplyPrefix(name);
	return set.GetEntryDetailed(transaction, new_name);
}

optional_ptr<CatalogEntry> ProxyCatalogSet::GetEntry(CatalogTransaction transaction, const string &name) {
	auto new_name = ApplyPrefix(name);
	return set.GetEntry(transaction, new_name);
}

void ProxyCatalogSet::Scan(CatalogTransaction transaction, const std::function<void(CatalogEntry &)> &callback) {
	set.ScanWithPrefix(
	    transaction,
	    [&](CatalogEntry &entry) {
		    auto &dep = entry.Cast<DependencyCatalogEntry>();
		    auto &name = dep.FromMangledName();
		    if (!StringUtil::CIEquals(name, mangled_name)) {
			    return;
		    }
		    callback(entry);
	    },
	    mangled_name);
}

bool ProxyCatalogSet::DropEntry(CatalogTransaction transaction, const string &name, bool cascade,
                                bool allow_drop_internal) {
	auto new_name = ApplyPrefix(name);
	return set.DropEntry(transaction, new_name, cascade, allow_drop_internal);
}

} // namespace duckdb
