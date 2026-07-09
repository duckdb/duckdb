#include "duckdb/main/external_resource_type_registry.hpp"

#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"

namespace duckdb {

ExternalResourceTypeRegistry &ExternalResourceTypeRegistry::Get(DatabaseInstance &db) {
	return db.GetExternalResourceTypeRegistry();
}

ExternalResourceTypeRegistry &ExternalResourceTypeRegistry::Get(ClientContext &context) {
	return ExternalResourceTypeRegistry::Get(*context.db);
}

void ExternalResourceTypeRegistry::Add(ExternalResourceType type) {
	lock_guard<mutex> guard(lock);
	types.push_back(std::move(type));
}

vector<ExternalResourceType> ExternalResourceTypeRegistry::List() const {
	lock_guard<mutex> guard(lock);
	return types;
}

unique_ptr<ExternalResourceType> ExternalResourceTypeRegistry::Lookup(const string &name) const {
	lock_guard<mutex> guard(lock);
	// Latest-wins: scan newest-first so a later registration shadows an earlier one.
	for (idx_t i = types.size(); i > 0; i--) {
		if (types[i - 1].name == name) {
			return make_uniq<ExternalResourceType>(types[i - 1]);
		}
	}
	return nullptr;
}

} // namespace duckdb
