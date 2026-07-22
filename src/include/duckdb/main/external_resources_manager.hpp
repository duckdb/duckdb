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

//! An external resource this instance tracks, registered under a local name (from CREATE/REGISTER EXTERNAL
//! RESOURCE). `name` is the local alias and the only key enforced here: `(type, handle)` identifies the
//! external thing, but the handle is opaque - two different maps may well denote the same resource, and only
//! the provider could say, at the cost of a round-trip that is not always possible. The rest is the binding
//! kept for display + teardown: from the type's status callback on create, from the caller on register.
//! Parallels ExternalResourceType (the recipe) — one provisioned instance of it.
struct ExternalResource {
	//! The local registration name.
	string name;
	//! The resource type (provider) that provisioned it — the key into the type registry.
	string type;
	//! Opaque create handle: the resource's identity, passed back to status/destroy.
	Value handle;
	//! Endpoint + db type from the ready status result (for display / later ATTACH). May be empty.
	string uri;
	string attached_db_type;
	//! Deleter binding for teardown: `<deleter_function>(<deleter_payload>)`. deleter_payload is the handle.
	//! Stored fully qualified where possible, see QualifyTableCallback.
	string deleter_function;
	Value deleter_payload;
};

//! Resolve a table-producing callback (a table function or table macro) to its fully-qualified
//! `catalog.schema.name` in `context`'s search path, so it stays resolvable from any other connection later:
//! teardown runs the deleter on a separate internal connection, under a search path that need not match the
//! one in effect at registration. Returns empty if the callback cannot be resolved.
string QualifyTableCallback(ClientContext &context, const string &name);

//! In-memory, instance-scoped manager of external resource instances (shared across connections). The
//! external resources themselves are durable, but this manager is not: it is the instance's local view of
//! what it manages, rebuilt across restarts via REGISTER. A name is claimed by its first registration.
//! Deliberately not transactional: the resource exists out in the world whether or not the transaction that
//! registered it commits, so dropping the record on rollback would strand it with nothing left to reap it.
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
