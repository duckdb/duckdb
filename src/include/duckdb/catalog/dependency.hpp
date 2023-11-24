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

struct DependencyFlags {
private:
	static constexpr uint8_t NON_BLOCKING = 0;
	static constexpr uint8_t BLOCKING = 1 << 0;
	static constexpr uint8_t OWNED = 1 << 1;
	static constexpr uint8_t OWNERSHIP = 1 << 2;

public:
	DependencyFlags() : value(0) {
	}
	DependencyFlags(const DependencyFlags &other) : value(other.value) {
	}
	DependencyFlags &operator=(const DependencyFlags &other) {
		value = other.value;
		return *this;
	}
	bool operator==(const DependencyFlags &other) const {
		return other.value == value;
	}
	bool operator!=(const DependencyFlags &other) const {
		return !(*this == other);
	}

public:
	bool IsBlocking() const {
		return (value & BLOCKING) == BLOCKING;
	}

	bool IsOwned() const {
		return (value & OWNED) == OWNED;
	}

	bool IsOwnership() const {
		return (value & OWNERSHIP) == OWNERSHIP;
	}

public:
	DependencyFlags &SetOwnership() {
		value |= OWNERSHIP;
		return *this;
	}
	DependencyFlags &SetOwned() {
		value |= OWNED;
		return *this;
	}
	DependencyFlags &SetBlocking() {
		value |= BLOCKING;
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
	DependencyFlags &Apply(DependencyFlags other) {
		if (other.IsBlocking()) {
			SetBlocking();
		}
		if (other.IsOwned()) {
			D_ASSERT(!IsOwnership());
			SetOwned();
		}
		if (other.IsOwnership()) {
			D_ASSERT(!IsOwned());
			SetOwnership();
		}
		return *this;
	}

public:
	string ToString() const {
		string result;
		if (IsBlocking()) {
			result += "REGULAR";
		} else {
			result += "AUTOMATIC";
		}
		result += " | ";
		if (IsOwned()) {
			D_ASSERT(!IsOwnership());
			result += "OWNED BY";
		}
		if (IsOwnership()) {
			D_ASSERT(!IsOwned());
			result += "OWNS";
		}
		return result;
	}

public:
	uint8_t value;
};

struct Dependency {
	Dependency(CatalogEntry &entry, DependencyFlags flags = DependencyFlags().SetBlocking())
	    : // NOLINT: Allow implicit conversion from `CatalogEntry`
	      entry(entry), flags(flags) {
	}

	//! The catalog entry this depends on
	reference<CatalogEntry> entry;
	//! The type of dependency
	DependencyFlags flags;
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
