//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/extension/linked_extension_registry.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/winapi.hpp"

#include <functional>

namespace duckdb {
class DuckDB;

//! An extension compiled into this binary, as a name and the callable that loads it.
struct LinkedExtension {
	string name;
	std::function<void(DuckDB &)> load;
};

//! The extensions linked into this binary. Each of them carries a LinkedExtensionRegistrar that runs
//! before main, so the registry is complete by the time the first database is created and the engine
//! never has to know at build time what was linked next to it.
class LinkedExtensionRegistry {
public:
	DUCKDB_API static void Register(const string &name, std::function<void(DuckDB &)> load);
	//! A copy, in registration order
	DUCKDB_API static vector<LinkedExtension> Get();
};

//! Instantiate one at namespace scope to register an extension when the object holding it is loaded
struct LinkedExtensionRegistrar {
	LinkedExtensionRegistrar(const char *name, std::function<void(DuckDB &)> load) {
		LinkedExtensionRegistry::Register(name, std::move(load));
	}
};

} // namespace duckdb
