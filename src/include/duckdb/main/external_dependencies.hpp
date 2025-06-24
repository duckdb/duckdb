//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/external_dependencies.hpp
//
//
//===----------------------------------------------------------------------===//

#include "duckdb/common/case_insensitive_map.hpp"
#include <functional>

#pragma once

namespace duckdb {

class DependencyItem {
public:
	virtual ~DependencyItem() {};

public:
	template <class TARGET>
	TARGET &Cast() {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<TARGET &>(*this);
	}
	template <class TARGET>
	const TARGET &Cast() const {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<const TARGET &>(*this);
	}
};

using dependency_scan_t = std::function<void(const string &name, shared_ptr<DependencyItem> item)>;

class ExternalDependency {
public:
	explicit ExternalDependency() {
	}
	~ExternalDependency() {
	}

public:
	void AddDependency(const string &name, shared_ptr<DependencyItem> item) {
		objects[name] = std::move(item);
	}
	shared_ptr<DependencyItem> GetDependency(const string &name) const {
		auto it = objects.find(name);
		if (it == objects.end()) {
			return nullptr;
		}
		return it->second;
	}
	void ScanDependencies(const dependency_scan_t &callback) {
		for (auto &kv : objects) {
			callback(kv.first, kv.second);
		}
	}

private:
	//! The objects encompassed by this dependency
	case_insensitive_map_t<shared_ptr<DependencyItem>> objects;
};

} // namespace duckdb
