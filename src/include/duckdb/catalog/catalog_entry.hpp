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
#include <memory>

namespace duckdb {
struct AlterInfo;
class Catalog;
class CatalogSet;
class ClientContext;
class SchemaCatalogEntry;
class Serializer;
class Deserializer;

struct CreateInfo;

//! Abstract base class of an entry in the catalog
class CatalogEntry {
public:
	CatalogEntry(CatalogType type, Catalog &catalog, string name);
	CatalogEntry(CatalogType type, string name, idx_t oid);
	virtual ~CatalogEntry();

	//! The oid of the entry
	idx_t oid;
	//! The type of this catalog entry
	CatalogType type;
	//! Reference to the catalog set this entry is stored in
	optional_ptr<CatalogSet> set;
	//! The name of the entry
	string name;
	//! Whether or not the object is deleted
	bool deleted;
	//! Whether or not the object is temporary and should not be added to the WAL
	bool temporary;
	//! Whether or not the entry is an internal entry (cannot be deleted, not dumped, etc)
	bool internal;
	//! Timestamp at which the catalog entry was created
	atomic<transaction_t> timestamp;
	//! Child entry
	unique_ptr<CatalogEntry> child;
	//! Parent entry (the node that dependents_map this node)
	optional_ptr<CatalogEntry> parent;

public:
	virtual unique_ptr<CatalogEntry> AlterEntry(ClientContext &context, AlterInfo &info);
	virtual void UndoAlter(ClientContext &context, AlterInfo &info);

	virtual unique_ptr<CatalogEntry> Copy(ClientContext &context) const;

	virtual unique_ptr<CreateInfo> GetInfo() const;

	//! Sets the CatalogEntry as the new root entry (i.e. the newest entry)
	// this is called on a rollback to an AlterEntry
	virtual void SetAsRoot();

	//! Convert the catalog entry to a SQL string that can be used to re-construct the catalog entry
	virtual string ToSQL() const;

	virtual Catalog &ParentCatalog();
	virtual SchemaCatalogEntry &ParentSchema();

	virtual void Verify(Catalog &catalog);

	void Serialize(Serializer &serializer) const;
	static unique_ptr<CreateInfo> Deserialize(Deserializer &deserializer);

public:
	template <class TARGET>
	TARGET &Cast() {
		D_ASSERT(dynamic_cast<TARGET *>(this));
		return reinterpret_cast<TARGET &>(*this);
	}
	template <class TARGET>
	const TARGET &Cast() const {
		D_ASSERT(dynamic_cast<const TARGET *>(this));
		return reinterpret_cast<const TARGET &>(*this);
	}
};

class InCatalogEntry : public CatalogEntry {
public:
	InCatalogEntry(CatalogType type, Catalog &catalog, string name);
	~InCatalogEntry() override;

	//! The catalog the entry belongs to
	Catalog &catalog;

public:
	Catalog &ParentCatalog() override {
		return catalog;
	}

	void Verify(Catalog &catalog) override;
};

} // namespace duckdb
