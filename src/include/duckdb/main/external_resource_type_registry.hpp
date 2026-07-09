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

//! An external resource type: a named kind of external resource DuckDB can provision and reap (a
//! container, a cloud stack, ...), described purely by its lifecycle callbacks. Every field is a
//! simple, SQL-expressible value — resource types are data, not C++ objects. The callbacks are
//! referenced by name and resolved against the catalog lazily (when invoked), so they may be defined
//! independently. `WITH EXTERNAL RESOURCE '<type>' ... ATTACH|CONNECT` is the first consumer, but a type
//! says nothing about attaching — the backend/endpoint is what the `status` callback returns at
//! provision time.
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
