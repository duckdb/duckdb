//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/catalog/dependency_list.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/field_writer.hpp"
#include "duckdb/catalog/catalog_entry_map.hpp"
#include "duckdb/common/types/hash.hpp"

namespace duckdb {
class Catalog;
class CatalogEntry;
struct CreateInfo;
class SchemaCatalogEntry;
struct CatalogTransaction;
class LogicalDependencyList;

//! The DependencyList containing CatalogEntry references, looked up in the catalog
class PhysicalDependencyList {
	friend class DependencyManager;

public:
	DUCKDB_API void AddDependency(CatalogEntry &entry);

	DUCKDB_API void VerifyDependencies(Catalog &catalog, const string &name);

	DUCKDB_API bool Contains(CatalogEntry &entry);

	DUCKDB_API LogicalDependencyList GetLogical() const;

private:
	catalog_entry_set_t set;
};

//! A minimal representation of a CreateInfo / CatalogEntry
//! enough to look up the entry inside SchemaCatalogEntry::GetEntry
struct LogicalDependency {
public:
	string name;
	string schema;
	string catalog;
	CatalogType type;

public:
	LogicalDependency(CatalogEntry &entry);
	LogicalDependency();

public:
	void FormatSerialize(FormatSerializer &serializer) const;
	static LogicalDependency FormatDeserialize(FormatDeserializer &deserializer);
};

struct CreateInfoHashFunction {
	uint64_t operator()(const LogicalDependency &a) const;
};

struct CreateInfoEquality {
	bool operator()(const LogicalDependency &a, const LogicalDependency &b) const;
};

//! The DependencyList containing LogicalDependency objects, not looked up in the catalog yet
class LogicalDependencyList {
	using create_info_set_t = unordered_set<LogicalDependency, CreateInfoHashFunction, CreateInfoEquality>;

public:
	DUCKDB_API void AddDependency(LogicalDependency entry);
	DUCKDB_API void AddDependency(CatalogEntry &entry);
	DUCKDB_API bool Contains(LogicalDependency &entry);
	DUCKDB_API bool Contains(CatalogEntry &entry);
	DUCKDB_API PhysicalDependencyList GetPhysical(Catalog &catalog, ClientContext &context) const;
	DUCKDB_API PhysicalDependencyList GetPhysical(ClientContext &context) const;
	DUCKDB_API PhysicalDependencyList GetPhysical(optional_ptr<ClientContext> context) const;

public:
	void Serialize(Serializer &serializer) const;
	static LogicalDependencyList Deserialize(Deserializer &source);
	void FormatSerialize(FormatSerializer &serializer) const;
	static LogicalDependencyList FormatDeserialize(FormatDeserializer &deserializer);

private:
	create_info_set_t set;
};

} // namespace duckdb
