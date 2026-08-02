//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/external_resources_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/enums/external_resources_mode.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {
class DatabaseInstance;
class ClientContext;
struct DBConfig;

//! What a caller wants to do with the external resource feature. Every user-facing entry point names one,
//! and `ExternalResources::RequireCapability` maps it to the settings that govern it — this enum plus that
//! mapping is the extension point: a new capability is added here, not by adding checks at the callsites.
enum class ExternalResourceCapability : uint8_t {
	//! Reading what exists: duckdb_external_resources(), duckdb_external_resource_types(), SHOW EXTERNAL RESOURCES.
	LISTING,
	//! Provisioning and teardown: CREATE / REGISTER / DESTROY EXTERNAL RESOURCE and their table functions.
	MANAGEMENT,
	//! Registering a new external resource type.
	TYPE_REGISTRATION
};

//! The configuration gate for the external resource feature. The C++ APIs below (the manager, the type
//! registry) stay ungated on purpose: internal paths such as teardown must keep working, and an extension
//! that is already loaded is inside the trust boundary. Only the SQL entry points are gated - each of them
//! twice: at bind time so the failure is early and clear, and again where the work happens, because a
//! statement prepared before the setting was lowered would otherwise still run.
struct ExternalResources {
	//! Throws a PermissionException unless the configuration permits `capability`.
	static void RequireCapability(const DBConfig &config, ExternalResourceCapability capability);
	static void RequireCapability(ClientContext &context, ExternalResourceCapability capability);
};

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

//! A deleter binding for an external resource: runs `<deleter_function>(<deleter_payload>)` to tear the
//! resource down. Used to reap a resource on a failed CREATE/REGISTER and to run DESTROY.
class ResourceDeleter {
public:
	ResourceDeleter(DatabaseInstance &db, string deleter_function, Value deleter_payload, string resource_type,
	                string resource_name);

	//! The teardown query `SELECT * FROM <deleter_function>(<deleter_payload>)`, with the function name
	//! safely quoted. Empty if there is no deleter. The single source of the teardown SQL.
	string DeleteSQL() const;
	//! Run the teardown on a private internal connection; throws on failure, with a retry hint.
	void Delete();
	//! Best-effort teardown: logs a warning on failure instead of throwing.
	void TryDelete();

private:
	DatabaseInstance &db;
	string deleter_function;
	Value deleter_payload;
	string resource_type;
	string resource_name;
};

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
