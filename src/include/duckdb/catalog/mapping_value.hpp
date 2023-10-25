//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/catalog/mapping_value.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog_entry.hpp"
#include "duckdb/catalog/catalog_set.hpp"

namespace duckdb {
struct AlterInfo;

class ClientContext;

struct EntryIndex {
	EntryIndex() : catalog(nullptr), index(DConstants::INVALID_INDEX) {
	}
	EntryIndex(CatalogSet &catalog, catalog_entry_t index) : catalog(&catalog), index(index) {
		auto entry = catalog.entries.find(index);
		if (entry == catalog.entries.end()) {
			throw InternalException("EntryIndex - Catalog entry not found in constructor!?");
		}
		catalog.entries.at(index).IncreaseRefCount();
	}
	~EntryIndex() {
		if (!catalog) {
			return;
		}
		auto entry = catalog->entries.find(index);
		D_ASSERT(entry != catalog->entries.end());
		const bool reached_zero = entry->second.DecreaseRefCount();
		if (reached_zero) {
			catalog->entries.erase(index);
		}
		catalog = nullptr;
	}
	// disable copy constructors
	EntryIndex(const EntryIndex &other) = delete;
	EntryIndex &operator=(const EntryIndex &) = delete;
	//! enable move constructors
	EntryIndex(EntryIndex &&other) noexcept {
		catalog = nullptr;
		index = DConstants::INVALID_INDEX;
		std::swap(catalog, other.catalog);
		std::swap(index, other.index);
	}
	EntryIndex &operator=(EntryIndex &&other) noexcept {
		std::swap(catalog, other.catalog);
		std::swap(index, other.index);
		return *this;
	}

private:
	EntryValue &GetEntryInternal(catalog_entry_t index) {
		auto entry = catalog->entries.find(index);
		if (entry == catalog->entries.end()) {
			throw InternalException("EntryIndex - Catalog entry not found!?");
		}
		return entry->second;
	}

public:
	CatalogEntry &GetEntry() {
		auto &entry_value = GetEntryInternal(index);
		return entry_value.Entry();
	}
	unique_ptr<CatalogEntry> TakeEntry() {
		auto &entry_value = GetEntryInternal(index);
		return entry_value.TakeEntry();
	}
	void SetEntry(unique_ptr<CatalogEntry> entry) {
		auto &entry_value = GetEntryInternal(index);
		entry_value.SetEntry(std::move(entry));
	}
	catalog_entry_t GetIndex() {
		return index;
	}
	EntryIndex Copy() {
		if (catalog) {
			return EntryIndex(*catalog, index);
		} else {
			return EntryIndex();
		}
	}

private:
	CatalogSet *catalog;
	catalog_entry_t index;
};

struct MappingValue {
	explicit MappingValue(EntryIndex index_p)
	    : index(std::move(index_p)), timestamp(0), deleted(false), parent(nullptr) {
	}

	EntryIndex index;
	transaction_t timestamp;
	bool deleted;
	unique_ptr<MappingValue> child;
	MappingValue *parent;
};

} // namespace duckdb
