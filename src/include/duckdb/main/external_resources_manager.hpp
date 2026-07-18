//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/external_resources_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/mutex.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {
class DatabaseInstance;
class ClientContext;

//! A provisioned external resource this instance owns, registered under a local name (from CREATE/REGISTER
//! EXTERNAL RESOURCE). The durable identity is `(type, handle)`: everything else is the binding derived
//! from it by the type's status/destroy callbacks and cached here for display + teardown. Parallels
//! ExternalResourceType (the recipe) — this is one provisioned instance of it.
struct ExternalResource {
	//! The local registration name (e.g. `yoooo`).
	string name;
	//! The resource type (provider) that provisioned it — the key into the type registry.
	string type;
	//! Opaque create handle: the resource's identity, passed back to status/destroy.
	Value handle;
	//! Endpoint + db type from the ready status result (for display / later ATTACH). May be empty.
	string uri;
	string attached_db_type;
	//! Deleter binding for teardown: `<deleter_function>(<deleter_payload>)`. deleter_payload is the handle.
	string deleter_function;
	Value deleter_payload;
};

//! In-memory, instance-scoped manager of external resource instances (shared across connections). The
//! external resources themselves are durable, but this manager is not: it is the instance's local view of
//! what it manages, rebuilt across restarts via REGISTER. A name is claimed by its first registration.
class ExternalResourcesManager {
public:
	static ExternalResourcesManager &Get(DatabaseInstance &db);
	static ExternalResourcesManager &Get(ClientContext &context);

	//! Register an instance. Throws if the name is already registered.
	void Add(ExternalResource instance);
	//! Remove the instance with the given name. Returns it if present, nullptr otherwise.
	unique_ptr<ExternalResource> Remove(const string &name);
	//! The registered instance with the given name, or nullptr if none.
	unique_ptr<ExternalResource> Lookup(const string &name) const;
	//! Snapshot of all registered instances, in registration order.
	vector<ExternalResource> List() const;

private:
	mutable mutex lock;
	vector<ExternalResource> instances;
};

} // namespace duckdb
