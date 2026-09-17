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

//! The extensions linked into this binary, registered through duckdb_register_static_extension.
class LinkedExtensionRegistry {
public:
	//! A copy, in registration order. Throws if a registration failed.
	DUCKDB_API static vector<LinkedExtension> Get();
};

} // namespace duckdb
