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

namespace duckdb {
class Catalog;
class CatalogEntry;

//! The DependencyList
class DependencyList {
	friend class DependencyManager;

public:
	DUCKDB_API void AddDependency(CatalogEntry &entry);

	DUCKDB_API void VerifyDependencies(Catalog &catalog, const string &name);

	DUCKDB_API bool Contains(CatalogEntry &entry);
public:
	void FormatSerialize(FormatSerializer &serializer) const;
	static DependencyList FormatDeserialize(FormatDeserializer &serializer);

	void Serialize(Serializer &serializer) const;
	static DependencyList Deserialize(Deserializer &deserializer);
	
private:
	catalog_entry_set_t set;
};
} // namespace duckdb
