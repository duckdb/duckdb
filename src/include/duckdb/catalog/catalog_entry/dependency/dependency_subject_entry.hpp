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

class DependencySubjectEntry : public DependencyEntry {
public:
	~DependencySubjectEntry() override;
	DependencySubjectEntry(Catalog &catalog, const DependencyInfo &info);

public:
	const CatalogEntryInfo &EntryInfo() const override;
	const MangledEntryName &EntryMangledName() const override;
};

} // namespace duckdb
