#include "duckdb/storage/read_policy_registry.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/common/exception.hpp"

namespace duckdb {

// Factory functions for built-in policies
static unique_ptr<ReadPolicy> CreateDefaultReadPolicy() {
	return make_uniq<DefaultReadPolicy>();
}

static unique_ptr<ReadPolicy> CreateAlignedReadPolicy() {
	return make_uniq<AlignedReadPolicy>();
}

ReadPolicyRegistry::ReadPolicyRegistry(DatabaseInstance &db) : db(db) {
	// Register built-in policies
	RegisterPolicy("default", CreateDefaultReadPolicy);
	RegisterPolicy("aligned", CreateAlignedReadPolicy);
}

ReadPolicyRegistry::~ReadPolicyRegistry() {
}

void ReadPolicyRegistry::RegisterPolicy(const string &name, read_policy_factory_t factory) {
	lock_guard<mutex> guard(lock);
	auto name_lower = StringUtil::Lower(name);
	if (policies.find(name_lower) != policies.end()) {
		throw InvalidInputException("Read policy '%s' is already registered", name);
	}
	policies[name_lower] = factory;
}

unique_ptr<ReadPolicy> ReadPolicyRegistry::CreatePolicy(const string &name) {
	lock_guard<mutex> guard(lock);
	auto name_lower = StringUtil::Lower(name);
	auto it = policies.find(name_lower);
	if (it == policies.end()) {
		throw InvalidInputException("Unknown read policy '%s'. Valid options are: %s", name,
		                            StringUtil::Join(GetPolicyNames(), ", "));
	}
	return it->second();
}

bool ReadPolicyRegistry::HasPolicy(const string &name) {
	lock_guard<mutex> guard(lock);
	auto name_lower = StringUtil::Lower(name);
	return policies.find(name_lower) != policies.end();
}

vector<string> ReadPolicyRegistry::GetPolicyNames() {
	// Note: caller should hold lock
	vector<string> names;
	for (auto &entry : policies) {
		names.push_back(entry.first);
	}
	return names;
}

ReadPolicyRegistry &ReadPolicyRegistry::Get(DatabaseInstance &db) {
	return db.GetReadPolicyRegistry();
}

} // namespace duckdb
