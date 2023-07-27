//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/catalog/catalog_set.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog_entry.hpp"
#include "duckdb/catalog/default/default_generator.hpp"
#include "duckdb/common/common.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/pair.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/parser/column_definition.hpp"
#include "duckdb/transaction/transaction.hpp"
#include "duckdb/catalog/catalog_transaction.hpp"
#include "duckdb/catalog/similar_catalog_entry.hpp"
#include <functional>
#include <memory>

namespace duckdb {
struct AlterInfo;

class ClientContext;
class DependencyList;
struct MappingValue;
struct EntryIndex;

class DuckCatalog;
class TableCatalogEntry;
class SequenceCatalogEntry;

typedef unordered_map<CatalogSet *, unique_lock<mutex>> set_lock_map_t;

struct EntryValue {
	EntryValue() {
		throw InternalException("EntryValue called without a catalog entry");
	}

	explicit EntryValue(unique_ptr<CatalogEntry> entry_p) : entry(std::move(entry_p)), reference_count(0) {
	}
	//! enable move constructors
	EntryValue(EntryValue &&other) noexcept {
		Swap(other);
	}
	EntryValue &operator=(EntryValue &&other) noexcept {
		Swap(other);
		return *this;
	}
	void Swap(EntryValue &other) {
		std::swap(entry, other.entry);
		idx_t count = reference_count;
		reference_count = other.reference_count.load();
		other.reference_count = count;
	}

	unique_ptr<CatalogEntry> entry;
	atomic<idx_t> reference_count;
};

//! The Catalog Set stores (key, value) map of a set of CatalogEntries
class CatalogSet {
	friend class DependencyManager;
	friend class EntryDropper;
	friend struct EntryIndex;

public:
	DUCKDB_API explicit CatalogSet(Catalog &catalog, unique_ptr<DefaultGenerator> defaults = nullptr);
	~CatalogSet();

	//! Create an entry in the catalog set. Returns whether or not it was
	//! successful.
	DUCKDB_API bool CreateEntry(CatalogTransaction transaction, const string &name, unique_ptr<CatalogEntry> value,
	                            DependencyList &dependencies);
	DUCKDB_API bool CreateEntry(ClientContext &context, const string &name, unique_ptr<CatalogEntry> value,
	                            DependencyList &dependencies);

	DUCKDB_API bool AlterEntry(CatalogTransaction transaction, const string &name, AlterInfo &alter_info);

	DUCKDB_API bool DropEntry(CatalogTransaction transaction, const string &name, bool cascade,
	                          bool allow_drop_internal = false);
	DUCKDB_API bool DropEntry(ClientContext &context, const string &name, bool cascade,
	                          bool allow_drop_internal = false);

	DUCKDB_API DuckCatalog &GetCatalog();

	bool AlterOwnership(CatalogTransaction transaction, ChangeOwnershipInfo &info);

	void CleanupEntry(CatalogEntry &catalog_entry);

	//! Returns the entry with the specified name
	DUCKDB_API optional_ptr<CatalogEntry> GetEntry(CatalogTransaction transaction, const string &name);
	DUCKDB_API optional_ptr<CatalogEntry> GetEntry(ClientContext &context, const string &name);

	//! Gets the entry that is most similar to the given name (i.e. smallest levenshtein distance), or empty string if
	//! none is found. The returned pair consists of the entry name and the distance (smaller means closer).
	SimilarCatalogEntry SimilarEntry(CatalogTransaction transaction, const string &name);

	//! Rollback <entry> to be the currently valid entry for a certain catalog
	//! entry
	void Undo(CatalogEntry &entry);

	//! Scan the catalog set, invoking the callback method for every committed entry
	DUCKDB_API void Scan(const std::function<void(CatalogEntry &)> &callback);
	//! Scan the catalog set, invoking the callback method for every entry
	DUCKDB_API void Scan(CatalogTransaction transaction, const std::function<void(CatalogEntry &)> &callback);
	DUCKDB_API void Scan(ClientContext &context, const std::function<void(CatalogEntry &)> &callback);

	template <class T>
	vector<reference<T>> GetEntries(CatalogTransaction transaction) {
		vector<reference<T>> result;
		Scan(transaction, [&](CatalogEntry &entry) { result.push_back(entry.Cast<T>()); });
		return result;
	}

	DUCKDB_API bool HasConflict(CatalogTransaction transaction, transaction_t timestamp);
	DUCKDB_API bool UseTimestamp(CatalogTransaction transaction, transaction_t timestamp);

	void UpdateTimestamp(CatalogEntry &entry, transaction_t timestamp);

	void Verify(Catalog &catalog);

private:
	//! Given a root entry, gets the entry valid for this transaction
	CatalogEntry &GetEntryForTransaction(CatalogTransaction transaction, CatalogEntry &current);
	CatalogEntry &GetCommittedEntry(CatalogEntry &current);
	optional_ptr<CatalogEntry> GetEntryInternal(CatalogTransaction transaction, const string &name,
	                                            EntryIndex *entry_index);
	optional_ptr<CatalogEntry> GetEntryInternal(CatalogTransaction transaction, EntryIndex &entry_index);
	//! Drops an entry from the catalog set; must hold the catalog_lock to safely call this
	void DropEntryInternal(CatalogTransaction transaction, EntryIndex entry_index, CatalogEntry &entry, bool cascade);
	optional_ptr<CatalogEntry> CreateEntryInternal(CatalogTransaction transaction, unique_ptr<CatalogEntry> entry);
	optional_ptr<MappingValue> GetMapping(CatalogTransaction transaction, const string &name, bool get_latest = false);
	void PutMapping(CatalogTransaction transaction, const string &name, EntryIndex entry_index);
	void DeleteMapping(CatalogTransaction transaction, const string &name);
	void DropEntryDependencies(CatalogTransaction transaction, EntryIndex &entry_index, CatalogEntry &entry,
	                           bool cascade);

	//! Create all default entries
	void CreateDefaultEntries(CatalogTransaction transaction, unique_lock<mutex> &lock);
	//! Attempt to create a default entry with the specified name. Returns the entry if successful, nullptr otherwise.
	optional_ptr<CatalogEntry> CreateDefaultEntry(CatalogTransaction transaction, const string &name,
	                                              unique_lock<mutex> &lock);

	EntryIndex PutEntry(idx_t entry_index, unique_ptr<CatalogEntry> entry);
	void PutEntry(EntryIndex index, unique_ptr<CatalogEntry> entry);

private:
	DuckCatalog &catalog;
	//! The catalog lock is used to make changes to the data
	mutex catalog_lock;
	//! The set of catalog entries
	unordered_map<idx_t, EntryValue> entries;
	//! Mapping of string to catalog entry
	case_insensitive_map_t<unique_ptr<MappingValue>> mapping;
	//! The current catalog entry index
	idx_t current_entry = 0;
	//! The generator used to generate default internal entries
	unique_ptr<DefaultGenerator> defaults;
};
} // namespace duckdb
