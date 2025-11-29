#include "duckdb/storage/read_policy_registry.hpp"

#include <algorithm>

#include "duckdb/common/exception.hpp"
#include "duckdb/main/database.hpp"

namespace duckdb {

namespace {
unique_ptr<ReadPolicy> CreateDefaultReadPolicy() {
	return make_uniq<DefaultReadPolicy>();
}
unique_ptr<ReadPolicy> CreateAlignedReadPolicy() {
	return make_uniq<AlignedReadPolicy>();
}
} // namespace

ReadPolicyRegistry::ReadPolicyRegistry() {
	// Register built-in read policies
	RegisterPolicy("default", CreateDefaultReadPolicy);
	RegisterPolicy("aligned", CreateAlignedReadPolicy);
}

ReadPolicyRegistry::~ReadPolicyRegistry() {
}

void ReadPolicyRegistry::RegisterPolicy(const string &name, read_policy_factory_t factory) {
	lock_guard<mutex> guard(policy_lock);
	auto is_new = policies.emplace(name, factory).second;
	if (!is_new) {
		throw InvalidInputException("Read policy '%s' is already registered", name);
	}
}

unique_ptr<ReadPolicy> ReadPolicyRegistry::CreatePolicy(const string &name) {
	const lock_guard<mutex> guard(policy_lock);
	auto it = policies.find(name);
	if (it == policies.end()) {
		throw InvalidInputException("Unknown read policy '%s'. Valid options are: %s", name,
		                            StringUtil::Join(GetReadPolicies(), ", "));
	}
	return it->second();
}

bool ReadPolicyRegistry::HasPolicy(const string &name) {
	lock_guard<mutex> guard(policy_lock);
	return policies.find(name) != policies.end();
}

vector<string> ReadPolicyRegistry::GetReadPolicies() {
	vector<string> names;
	names.reserve(policies.size());
	for (auto &entry : policies) {
		names.emplace_back(entry.first);
	}
	// Sort to get a deterministic order.
	std::sort(names.begin(), names.end());
	return names;
}

/*static*/ ReadPolicyRegistry &ReadPolicyRegistry::Get(DatabaseInstance &db) {
	return db.GetReadPolicyRegistry();
}

} // namespace duckdb
