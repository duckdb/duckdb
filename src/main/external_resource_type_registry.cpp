#include "duckdb/main/external_resource_type_registry.hpp"

#include "duckdb/common/exception.hpp"
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
	// First-wins: a name is claimed by its first registration and cannot be redefined.
	for (auto &existing : types) {
		if (existing.name == type.name) {
			throw InvalidInputException("external resource type \"%s\" is already registered", type.name);
		}
	}
	types.push_back(std::move(type));
}

vector<ExternalResourceType> ExternalResourceTypeRegistry::List() const {
	lock_guard<mutex> guard(lock);
	return types;
}

unique_ptr<ExternalResourceType> ExternalResourceTypeRegistry::Lookup(const string &name) const {
	lock_guard<mutex> guard(lock);
	// Names are unique (see Add), so there is at most one match.
	for (auto &type : types) {
		if (type.name == name) {
			return make_uniq<ExternalResourceType>(type);
		}
	}
	return nullptr;
}

} // namespace duckdb
