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

class DependencyDependentEntry : public DependencyEntry {
public:
	~DependencyDependentEntry() override;
	DependencyDependentEntry(Catalog &catalog, const DependencyInfo &info);

public:
	const CatalogEntryInfo &EntryInfo() const override;
	const MangledEntryName &EntryMangledName() const override;
	const CatalogEntryInfo &SourceInfo() const override;
	const MangledEntryName &SourceMangledName() const override;
};

} // namespace duckdb
