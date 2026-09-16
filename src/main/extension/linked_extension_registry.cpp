#include "duckdb/main/extension/linked_extension_registry.hpp"

#include <mutex>

namespace duckdb {

namespace {

struct RegistryState {
	std::mutex lock;
	vector<LinkedExtension> extensions;
};

// function-local so that registrars running during static initialization always find it constructed
RegistryState &GetRegistryState() {
	static RegistryState state;
	return state;
}

} // namespace

void LinkedExtensionRegistry::Register(const string &name, std::function<void(DuckDB &)> load) {
	auto &state = GetRegistryState();
	std::lock_guard<std::mutex> guard(state.lock);
	state.extensions.push_back({name, std::move(load)});
}

vector<LinkedExtension> LinkedExtensionRegistry::Get() {
	auto &state = GetRegistryState();
	std::lock_guard<std::mutex> guard(state.lock);
	return state.extensions;
}

} // namespace duckdb
