//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/read_policy_registry.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/common.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/storage/read_policy.hpp"

namespace duckdb {

class DatabaseInstance;

//! Factory function type for creating read policies
typedef unique_ptr<ReadPolicy> (*read_policy_factory_t)();

//! Registry for read policies that can be extended by extensions
class ReadPolicyRegistry {
public:
	DUCKDB_API explicit ReadPolicyRegistry(DatabaseInstance &db);
	DUCKDB_API ~ReadPolicyRegistry();

	//! Register a new read policy
	DUCKDB_API void RegisterPolicy(const string &name, read_policy_factory_t factory);

	//! Create a read policy by name
	DUCKDB_API unique_ptr<ReadPolicy> CreatePolicy(const string &name);

	//! Check if a policy is registered
	DUCKDB_API bool HasPolicy(const string &name);

	//! Get all registered policy names
	DUCKDB_API vector<string> GetPolicyNames();

	//! Get the global registry for a database instance
	DUCKDB_API static ReadPolicyRegistry &Get(DatabaseInstance &db);

private:
	DatabaseInstance &db;
	case_insensitive_map_t<read_policy_factory_t> policies;
	mutex policy_lock;
};

} // namespace duckdb
