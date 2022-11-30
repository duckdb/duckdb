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
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/sequence_catalog_entry.hpp"
#include "duckdb/transaction/transaction.hpp"
#include <functional>
#include <memory>

namespace duckdb {
struct AlterInfo;

class ClientContext;
struct MappingValue;
struct EntryIndex;

typedef unordered_map<CatalogSet *, unique_lock<mutex>> set_lock_map_t;

struct EntryValue {
	EntryValue() {
		throw InternalException("EntryValue called without a catalog entry");
	}

	explicit EntryValue(unique_ptr<CatalogEntry> entry_p) : entry(move(entry_p)), reference_count(0) {
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
	DUCKDB_API bool CreateEntry(ClientContext &context, const string &name, unique_ptr<CatalogEntry> value,
	                            unordered_set<CatalogEntry *> &dependencies);

	DUCKDB_API bool AlterEntry(ClientContext &context, const string &name, AlterInfo *alter_info);

	DUCKDB_API bool DropEntry(ClientContext &context, const string &name, bool cascade);

	bool AlterOwnership(ClientContext &context, ChangeOwnershipInfo *info);

	void CleanupEntry(CatalogEntry *catalog_entry);

	//! Returns the entry with the specified name
	DUCKDB_API CatalogEntry *GetEntry(ClientContext &context, const string &name);

	//! Gets the entry that is most similar to the given name (i.e. smallest levenshtein distance), or empty string if
	//! none is found. The returned pair consists of the entry name and the distance (smaller means closer).
	pair<string, idx_t> SimilarEntry(ClientContext &context, const string &name);

	//! Rollback <entry> to be the currently valid entry for a certain catalog
	//! entry
	void Undo(CatalogEntry *entry);

	//! Scan the catalog set, invoking the callback method for every committed entry
	DUCKDB_API void Scan(const std::function<void(CatalogEntry *)> &callback);
	//! Scan the catalog set, invoking the callback method for every entry
	DUCKDB_API void Scan(ClientContext &context, const std::function<void(CatalogEntry *)> &callback);

	template <class T>
	vector<T *> GetEntries(ClientContext &context) {
		vector<T *> result;
		Scan(context, [&](CatalogEntry *entry) { result.push_back((T *)entry); });
		return result;
	}

	DUCKDB_API static bool HasConflict(ClientContext &context, transaction_t timestamp);
	DUCKDB_API static bool UseTimestamp(ClientContext &context, transaction_t timestamp);

	void UpdateTimestamp(CatalogEntry *entry, transaction_t timestamp);

private:
	//! Adjusts table dependencies on the event of an UNDO
	void AdjustTableDependencies(CatalogEntry *entry);
	//! Adjust one dependency
	void AdjustDependency(CatalogEntry *entry, TableCatalogEntry *table, ColumnDefinition &column, bool remove);
	//! Adjust User dependency
	void AdjustUserDependency(CatalogEntry *entry, ColumnDefinition &column, bool remove);
	//! Given a root entry, gets the entry valid for this transaction
	CatalogEntry *GetEntryForTransaction(ClientContext &context, CatalogEntry *current);
	CatalogEntry *GetCommittedEntry(CatalogEntry *current);
	bool GetEntryInternal(ClientContext &context, const string &name, EntryIndex *entry_index, CatalogEntry *&entry);
	bool GetEntryInternal(ClientContext &context, EntryIndex &entry_index, CatalogEntry *&entry);
	//! Drops an entry from the catalog set; must hold the catalog_lock to safely call this
	void DropEntryInternal(ClientContext &context, EntryIndex entry_index, CatalogEntry &entry, bool cascade);
	CatalogEntry *CreateEntryInternal(ClientContext &context, unique_ptr<CatalogEntry> entry);
	MappingValue *GetMapping(ClientContext &context, const string &name, bool get_latest = false);
	void PutMapping(ClientContext &context, const string &name, EntryIndex entry_index);
	void DeleteMapping(ClientContext &context, const string &name);
	void DropEntryDependencies(ClientContext &context, EntryIndex &entry_index, CatalogEntry &entry, bool cascade);

	//! Create all default entries
	void CreateDefaultEntries(ClientContext &context, unique_lock<mutex> &lock);
	//! Attempt to create a default entry with the specified name. Returns the entry if successful, nullptr otherwise.
	CatalogEntry *CreateDefaultEntry(ClientContext &context, const string &name, unique_lock<mutex> &lock);

	EntryIndex PutEntry(idx_t entry_index, unique_ptr<CatalogEntry> entry);
	void PutEntry(EntryIndex index, unique_ptr<CatalogEntry> entry);

private:
	Catalog &catalog;
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
