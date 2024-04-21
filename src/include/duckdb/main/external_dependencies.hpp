//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/external_dependencies.hpp
//
//
//===----------------------------------------------------------------------===//

#include "duckdb/common/case_insensitive_map.hpp"

#pragma once

namespace duckdb {

enum class ExternalDependencyItemType : uint8_t { PYTHON_DEPENDENCY };

class DependencyItem {
public:
	virtual ~DependencyItem() {};

public:
	ExternalDependencyItemType type;

public:
	template <class TARGET>
	TARGET &Cast() {
		if (type != TARGET::TYPE) {
			throw InternalException("Failed to cast DependencyItem to type - DependencyItem type mismatch");
		}
		return reinterpret_cast<TARGET &>(*this);
	}

	template <class TARGET>
	const TARGET &Cast() const {
		if (type != TARGET::TYPE) {
			throw InternalException("Failed to cast DependencyItem to type - DependencyItem type mismatch");
		}
		return reinterpret_cast<const TARGET &>(*this);
	}

protected:
	explicit DependencyItem(ExternalDependencyItemType type_p) : type(type_p) {
	}
};

class ExternalDependency {
public:
	explicit ExternalDependency() {
	}

public:
	virtual void AddDependency(const string &name, shared_ptr<DependencyItem> item) {
		objects[name] = std::move(item);
	}
	virtual shared_ptr<DependencyItem> GetDependency(const string &name) const {
		auto it = objects.find(name);
		if (it == objects.end()) {
			return nullptr;
		}
		return it->second;
	}
	virtual void ScanDependencies(std::function<void(const string &name, shared_ptr<DependencyItem> item)> callback) {
		for (auto &kv : objects) {
			callback(kv.first, kv.second);
		}
	}

private:
	//! The objects encompassed by this dependency
	case_insensitive_map_t<shared_ptr<DependencyItem>> objects;
};

//! Not actually storing any dependencies, just forwards to an existing ExternalDependency object
class ProxyDependencies : public ExternalDependency {
public:
	ProxyDependencies(shared_ptr<ExternalDependency> other) : other(other) {
	}
	void AddDependency(const string &name, shared_ptr<DependencyItem> item) override {
		other->AddDependency(name, std::move(item));
	}
	shared_ptr<DependencyItem> GetDependency(const string &name) const override {
		return other->GetDependency(name);
	}
	void ScanDependencies(std::function<void(const string &name, shared_ptr<DependencyItem> item)> callback) override {
		other->ScanDependencies(callback);
	}

public:
	shared_ptr<ExternalDependency> other;
};

} // namespace duckdb
