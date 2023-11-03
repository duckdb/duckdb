//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/catalog/dependency.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/unordered_set.hpp"

namespace duckdb {
class CatalogEntry;

enum class DependencyType : uint8_t {
	DEPENDENCY_REGULAR = 0,
	DEPENDENCY_AUTOMATIC = 1,
	DEPENDENCY_OWNS = 2,
	DEPENDENCY_OWNED_BY = 3
};

struct DependencyFlags {
private:
	static constexpr uint8_t NON_BLOCKING = 0;
	static constexpr uint8_t BLOCKING = 1;
	static constexpr uint8_t OWNED = 2;
	static constexpr uint8_t OWNERSHIP = 4;
public:
	DependencyFlags() : value(0) {}
public:
	bool IsBlocking() const {
		return value & BLOCKING;
	}
	bool IsOwned() const {
		return value & OWNED;
	}
	bool IsOwnership() const {
		return value & OWNERSHIP;
	}
public:
	DependencyFlags &SetOwnership() {
		value &= OWNERSHIP;
		return *this;
	}
	DependencyFlags &SetOwned() {
		value &= OWNED;
		return *this;
	}
	DependencyFlags &SetBlocking() {
		value &= BLOCKING;
		return *this;
	}
public:
	static DependencyFlags DependencyOwns() {
		return DependencyFlags().SetOwnership();
	}
	static DependencyFlags DependencyOwned() {
		return DependencyFlags().SetOwned();
	}
	static DependencyFlags DependencyAutomatic() {
		return DependencyFlags();
	}
	static DependencyFlags DependencyRegular() {
		return DependencyFlags().SetBlocking();
	}
public:
	uint8_t value;
};

struct Dependency {
	Dependency(CatalogEntry &entry, DependencyType dependency_type = DependencyType::DEPENDENCY_REGULAR)
	    : // NOLINT: Allow implicit conversion from `CatalogEntry`
	      entry(entry), dependency_type(dependency_type) {
	}

	//! The catalog entry this depends on
	reference<CatalogEntry> entry;
	//! The type of dependency
	DependencyType dependency_type;
};

struct DependencyHashFunction {
	uint64_t operator()(const Dependency &a) const {
		std::hash<void *> hash_func;
		return hash_func((void *)&a.entry.get());
	}
};

struct DependencyEquality {
	bool operator()(const Dependency &a, const Dependency &b) const {
		return RefersToSameObject(a.entry, b.entry);
	}
};
using dependency_set_t = unordered_set<Dependency, DependencyHashFunction, DependencyEquality>;

} // namespace duckdb
