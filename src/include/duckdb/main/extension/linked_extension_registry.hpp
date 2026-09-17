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
#include "duckdb_static_extension.h"

#include <functional>

namespace duckdb {
class DuckDB;

//! An extension compiled into this binary, as a name and the callable that loads it.
struct LinkedExtension {
	string name;
	std::function<void(DuckDB &)> load;
};

//! What a root described, once validated.
struct StaticExtensionDescription {
	string name;
	string version;
	duckdb_extension_descriptor descriptor;
};

//! The extensions linked into this binary, registered through duckdb_register_static_extension.
class LinkedExtensionRegistry {
public:
	//! Calls root and validates the descriptor it fills. Returns the reason on failure, otherwise an empty string.
	DUCKDB_API static string Describe(duckdb_extension_root root, StaticExtensionDescription &result);
	//! A copy, in registration order. Throws if a registration failed.
	DUCKDB_API static vector<LinkedExtension> Get();
};

} // namespace duckdb
