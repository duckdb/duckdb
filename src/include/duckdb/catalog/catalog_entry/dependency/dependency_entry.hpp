//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/catalog/catalog_entry.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/enums/catalog_type.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/atomic.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/catalog/catalog_entry.hpp"
#include "duckdb/catalog/catalog_set.hpp"
#include "duckdb/catalog/dependency.hpp"
#include "duckdb/catalog/dependency_manager.hpp"
#include <memory>

namespace duckdb {

class DependencyManager;

class DependencySetCatalogEntry;

//! Resembles a connection between an object and the CatalogEntry that can be retrieved from the Catalog using the
//! identifiers listed here

enum class DependencyEntryType : uint8_t { SUBJECT, RELIANT };

class DependencyEntry : public InCatalogEntry {
public:
	~DependencyEntry() override;

protected:
	DependencyEntry(Catalog &catalog, DependencyEntryType type, const MangledDependencyName &name,
	                const DependencyInfo &info);

public:
	const MangledEntryName &DependencyMangledName() const;
	const DependencySubject &Subject() const;

	const MangledEntryName &DependentMangledName() const;
	const DependencyReliant &Reliant() const;

	virtual const CatalogEntryInfo &EntryInfo() const = 0;
	virtual const MangledEntryName &EntryMangledName() const = 0;

public:
	DependencyEntryType Side() const;

protected:
	const MangledEntryName dependent_name;
	const MangledEntryName dependency_name;
	const DependencyReliant dependent;
	const DependencySubject dependency;

private:
	DependencyEntryType side;
};

} // namespace duckdb
