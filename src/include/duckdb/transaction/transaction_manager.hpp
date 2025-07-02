//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/transaction/transaction_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog_set.hpp"
#include "duckdb/common/common.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/error_data.hpp"
#include "duckdb/common/atomic.hpp"
#include <iostream>
#include <vector>
#include <unordered_map>
#include <cstddef>
#include <shared_mutex>

namespace duckdb {

class AttachedDatabase;
class ClientContext;
class Catalog;
struct ClientLockWrapper;
class DatabaseInstance;
class Transaction;

class Bitmap {
    std::vector<unsigned char> data;
	bool finalized = false;

public:
	std::vector<uint32_t> rids;

    Bitmap() : data(2048, 0) {};

    // Set bit at given index, auto-growing if needed
    void set(size_t index) {
        if (data.size() <= index) {
            data.resize(std::max(index + 1, data.size() * 2), 0);
        }
		if (finalized) {
			throw std::runtime_error("Cannot override bitmap after finalization");
		}
        data[index] = 1;
    }

	void finalize() {
		// Find the indexes of the bits that are set to 1 and put them in rids
		for (size_t i = 0; i < data.size(); ++i) {
			if (data[i]) {
				rids.push_back(i);
			}
		}
		finalized = true;
	}
};

class PredicateCache {
public:
	PredicateCache() = default;

	void Add(const std::string &table_name, const std::string &filter_fingerprint, const unsigned long offset, std::shared_ptr<Bitmap> bitmap) {
		std::unique_lock lock(predicateCacheMutex); // Exclusive write access
		internalCache[table_name][filter_fingerprint][offset] = bitmap;
	}

	// Get a bitmap from the cache by key
	const std::shared_ptr<Bitmap> Get(const std::string &table_name, const std::string &filter_fingerprint, const unsigned long offset) {
		std::shared_lock lock(predicateCacheMutex); // Shared read access

		// 1) Locate the table bucket
		auto tblIt = internalCache.find(table_name);
		if (tblIt == internalCache.end()) {
			return nullptr;
		}

		// 2) Locate the fingerprint bucket within that table
		auto fpIt = tblIt->second.find(filter_fingerprint);
		if (fpIt == tblIt->second.end()) {
			return nullptr;
		}

		// 3) Locate the bitmap at the requested offset
		auto offIt = fpIt->second.find(offset);
		if (offIt == fpIt->second.end()) {
			return nullptr;
		}

		return offIt->second;   // Success
	}

private:
	using TableName = std::string;
	using FilterFingerprint = std::string;
	using Offset = unsigned long;
	std::unordered_map<TableName, std::unordered_map<FilterFingerprint, unordered_map<Offset, std::shared_ptr<Bitmap>>>> internalCache;
	mutable std::shared_mutex predicateCacheMutex;
};

//! The Transaction Manager is responsible for creating and managing
//! transactions
class TransactionManager {
public:
	explicit TransactionManager(AttachedDatabase &db);
	virtual ~TransactionManager();

	//! Start a new transaction
	virtual Transaction &StartTransaction(ClientContext &context) = 0;
	//! Commit the given transaction. Returns a non-empty error message on failure.
	virtual ErrorData CommitTransaction(ClientContext &context, Transaction &transaction) = 0;
	//! Rollback the given transaction
	virtual void RollbackTransaction(Transaction &transaction) = 0;

	virtual void Checkpoint(ClientContext &context, bool force = false) = 0;

	static TransactionManager &Get(AttachedDatabase &db);

	virtual bool IsDuckTransactionManager() {
		return false;
	}

	AttachedDatabase &GetDB() {
		return db;
	}

	PredicateCache predicateCache;

protected:
	//! The attached database
	AttachedDatabase &db;
};

} // namespace duckdb
