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
class ReadPolicyRegistry final {
public:
	ReadPolicyRegistry();
	~ReadPolicyRegistry();

	//! Register a new read policy
	void RegisterPolicy(const string &name, read_policy_factory_t factory);

	//! Create a read policy by name
	//! If the policy name hasn't been registered before, InvalidInput exception is thrown.
	unique_ptr<ReadPolicy> CreatePolicy(const string &name);

	//! Check if a policy is registered
	bool HasPolicy(const string &name);

	//! Get all registered policy names
	vector<string> GetReadPolicies();

	//! Get the global registry for a database instance
	static ReadPolicyRegistry &Get(DatabaseInstance &db);

private:
	case_insensitive_map_t<read_policy_factory_t> policies;
	mutex policy_lock;
};

} // namespace duckdb
