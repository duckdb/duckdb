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
	EntryIndex(CatalogSet &catalog, idx_t index) : catalog(&catalog), index(index) {
		auto entry = catalog.entries.find(index);
		if (entry == catalog.entries.end()) {
			throw InternalException("EntryIndex - Catalog entry not found in constructor!?");
		}
		catalog.entries[index].reference_count++;
	}
	~EntryIndex() {
		if (!catalog) {
			return;
		}
		auto entry = catalog->entries.find(index);
		D_ASSERT(entry != catalog->entries.end());
		auto remaining_ref = --entry->second.reference_count;
		if (remaining_ref == 0) {
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

	unique_ptr<CatalogEntry> &GetEntry() {
		auto entry = catalog->entries.find(index);
		if (entry == catalog->entries.end()) {
			throw InternalException("EntryIndex - Catalog entry not found!?");
		}
		return entry->second.entry;
	}
	idx_t GetIndex() {
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
	idx_t index;
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
