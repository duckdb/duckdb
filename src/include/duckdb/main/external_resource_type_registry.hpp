//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/external_resource_type_registry.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {
class DatabaseInstance;
class ClientContext;

//! A named kind of external resource DuckDB can provision and reap, described purely by lifecycle callbacks
//! referenced by name. Every field is a simple string — resource types are data, resolved lazily when invoked.
struct ExternalResourceType {
	//! The type name, e.g. 'quack@aws:ec2'.
	string name;
	//! What kind of resource this is — a free-form classification set by the registrant. Consumers
	//! filter on it; e.g. `WITH EXTERNAL RESOURCE ... ATTACH|CONNECT` handles kind 'catalog', other
	//! kinds (compute, storage, ...) are provisioned but not attached. The registry itself is agnostic.
	string kind;
	//! Function that provisions the resource and returns a handle.
	string create_function;
	//! Function that probes readiness (handle -> state, endpoint, token, ...). May be empty.
	string status_function;
	//! Function that tears the resource down (handle -> void). May be empty.
	string destroy_function;
	//! Function that reconnects to an existing resource. May be empty.
	string resolve_function;
	//! Where it came from: "user" (register_external_resource_type) or "extension".
	string origin;
	//! The caller's catalog search path captured at registration (serialized). The callbacks are resolved
	//! against it when invoked — like views/macros capture their creation-time search path — so unqualified
	//! callback names keep resolving from create_external_resource's internal connection, regardless of the
	//! search path in effect when create runs. Empty means "use the internal connection's default".
	string search_path;
};

//! Append-only, in-memory, instance-scoped registry of external resource types (shared across
//! connections). Registration only ever appends; there is no removal or update. Lookup resolves the
//! most recently registered entry for a name (latest-wins is an internal lookup detail).
class ExternalResourceTypeRegistry {
public:
	static ExternalResourceTypeRegistry &Get(DatabaseInstance &db);
	static ExternalResourceTypeRegistry &Get(ClientContext &context);

	//! Append a resource type (append-only).
	void Add(ExternalResourceType type);
	//! Snapshot of all registered resource types, in registration order.
	vector<ExternalResourceType> List() const;
	//! Most recently registered type with the given name, or nullptr if none.
	unique_ptr<ExternalResourceType> Lookup(const string &name) const;

private:
	mutable mutex lock;
	vector<ExternalResourceType> types;
};

} // namespace duckdb
