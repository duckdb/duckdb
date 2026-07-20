#include "duckdb/main/external_resources_manager.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/entry_lookup_info.hpp"
#include "duckdb/common/enums/on_entry_not_found.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parser/qualified_name.hpp"

namespace duckdb {

string QualifyTableCallback(ClientContext &context, const string &name) {
	EntryLookupInfo table_fn(CatalogType::TABLE_FUNCTION_ENTRY, QualifiedName::Parse(name));
	auto entry = Catalog::GetEntry(context, table_fn, OnEntryNotFound::RETURN_NULL);
	if (!entry) {
		EntryLookupInfo table_macro(CatalogType::TABLE_MACRO_ENTRY, QualifiedName::Parse(name));
		entry = Catalog::GetEntry(context, table_macro, OnEntryNotFound::RETURN_NULL);
	}
	if (!entry) {
		return string();
	}
	return QualifiedName(entry->ParentCatalog().GetName(), entry->ParentSchema().name, entry->name).ToString();
}

ExternalResourcesManager &ExternalResourcesManager::Get(DatabaseInstance &db) {
	return db.GetExternalResourcesManager();
}

ExternalResourcesManager &ExternalResourcesManager::Get(ClientContext &context) {
	return ExternalResourcesManager::Get(*context.db);
}

void ExternalResourcesManager::Add(ExternalResource instance) {
	if (instance.name.empty()) {
		throw InvalidInputException("an external resource instance must have a name");
	}
	if (instance.type.empty()) {
		throw InvalidInputException("external resource \"%s\": a type is required", instance.name);
	}
	lock_guard<mutex> guard(lock);
	for (auto &existing : instances) {
		if (existing.name == instance.name) {
			throw InvalidInputException("external resource \"%s\" is already registered", instance.name);
		}
	}
	instances.push_back(std::move(instance));
}

unique_ptr<ExternalResource> ExternalResourcesManager::Remove(const string &name) {
	lock_guard<mutex> guard(lock);
	for (idx_t i = 0; i < instances.size(); i++) {
		if (instances[i].name == name) {
			auto result = make_uniq<ExternalResource>(std::move(instances[i]));
			instances.erase(instances.begin() + static_cast<int64_t>(i));
			return result;
		}
	}
	return nullptr;
}

unique_ptr<ExternalResource> ExternalResourcesManager::Lookup(const string &name) const {
	lock_guard<mutex> guard(lock);
	for (auto &instance : instances) {
		if (instance.name == name) {
			return make_uniq<ExternalResource>(instance);
		}
	}
	return nullptr;
}

vector<ExternalResource> ExternalResourcesManager::List() const {
	lock_guard<mutex> guard(lock);
	return instances;
}

} // namespace duckdb
