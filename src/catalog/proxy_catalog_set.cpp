#include "duckdb/catalog/proxy_catalog_set.hpp"

namespace duckdb {

static string ApplyPrefix(const string &prefix, const string &name) {
	return prefix + name;
}

bool ProxyCatalogSet::CreateEntry(CatalogTransaction transaction, const string &name, unique_ptr<CatalogEntry> value,
                                  const DependencyList &dependencies) {
	auto new_name = ApplyPrefix(prefix, name);
	value->name = new_name;
	return set.CreateEntry(transaction, new_name, std::move(value), dependencies);
}

optional_ptr<CatalogEntry> ProxyCatalogSet::GetEntry(CatalogTransaction transaction, const string &name) {
	auto new_name = ApplyPrefix(prefix, name);
	return set.GetEntry(transaction, new_name);
}

void ProxyCatalogSet::Scan(CatalogTransaction transaction, const std::function<void(CatalogEntry &)> &callback) {
	set.Scan(transaction, [&](CatalogEntry &entry) {
		if (!StringUtil::StartsWith(entry.name, prefix)) {
			return;
		}
		callback(entry);
	});
}

bool ProxyCatalogSet::DropEntry(CatalogTransaction transaction, const string &name, bool cascade,
                                bool allow_drop_internal) {
	auto new_name = ApplyPrefix(prefix, name);
	return set.DropEntry(transaction, new_name, cascade, allow_drop_internal);
}

} // namespace duckdb
