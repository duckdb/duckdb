//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/catalog/dependency_list.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog_entry_map.hpp"
#include "duckdb/common/types/hash.hpp"
#include "duckdb/common/enums/catalog_type.hpp"
#include "duckdb/catalog/dependency.hpp"

namespace duckdb {
class Catalog;
class CatalogEntry;
struct CreateInfo;
class SchemaCatalogEntry;
struct CatalogTransaction;
class LogicalDependencyList;

//! A minimal representation of a CreateInfo / CatalogEntry
//! enough to look up the entry inside SchemaCatalogEntry::GetEntry
struct LogicalDependency {
public:
	CatalogEntryInfo entry;
	string catalog;

public:
	explicit LogicalDependency(CatalogEntry &entry);
	explicit LogicalDependency(const string &catalog, const CatalogEntryInfo &entry);
	LogicalDependency();
	unique_ptr<LogicalDependency> Copy() const;
	bool operator==(const LogicalDependency &other) const;
};

struct LogicalDependencyHashFunction {
	uint64_t operator()(const LogicalDependency &a) const;
};

struct LogicalDependencyEquality {
	bool operator()(const LogicalDependency &a, const LogicalDependency &b) const;
};

//! The DependencyList containing LogicalDependency objects, not looked up in the catalog yet
class LogicalDependencyList {
	using create_info_set_t =
	    unordered_set<LogicalDependency, LogicalDependencyHashFunction, LogicalDependencyEquality>;

public:
	DUCKDB_API void AddDependency(CatalogEntry &entry);
	DUCKDB_API void AddDependency(const LogicalDependency &entry);
	DUCKDB_API bool Contains(CatalogEntry &entry);

public:
	DUCKDB_API void VerifyDependencies(Catalog &catalog, const string &name);
	bool operator==(const LogicalDependencyList &other) const;
	const create_info_set_t &Set() const;

private:
	create_info_set_t set;
};

} // namespace duckdb
