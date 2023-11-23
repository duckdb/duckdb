//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/catalog/catalog_entry.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog_entry/dependency/dependency_entry.hpp"

namespace duckdb {

class DependencyReliantEntry : public DependencyEntry {
public:
	~DependencyReliantEntry() override;
	DependencyReliantEntry(Catalog &catalog, const DependencyInfo &info);

public:
	const CatalogEntryInfo &EntryInfo() const override;
	const MangledEntryName &EntryMangledName() const override;
};

} // namespace duckdb
