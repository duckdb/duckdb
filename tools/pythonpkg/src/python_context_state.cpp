#include "duckdb_python/python_context_state.hpp"

namespace duckdb {

// Replacement Cache

ReplacementCache::ReplacementCache() {
}

unique_ptr<TableRef> ReplacementCache::Lookup(const string &name) {
	auto it = cache.find(name);
	if (it != cache.end()) {
		return it->second->Copy();
	}
	return nullptr;
}

void ReplacementCache::Add(const string &name, unique_ptr<TableRef> result) {
	D_ASSERT(result);
	cache.emplace(std::make_pair(name, std::move(result)));
}

void ReplacementCache::Evict() {
	cache.clear();
}

// Client Context State

PythonContextState::PythonContextState() {
}

} // namespace duckdb
