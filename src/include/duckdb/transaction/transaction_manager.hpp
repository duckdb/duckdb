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

class PredicateCache {
public:
    using TableName         = std::string;
    using FilterFingerprint = std::string;
    using Offset            = unsigned long;
    using CacheKey          = std::pair<TableName, FilterFingerprint>;

private:
	struct CacheKeyHash {
		std::size_t operator()(const CacheKey& k) const noexcept {
			// Basic 64-bit hash combine
			std::size_t h1 = std::hash<std::string>{}(k.first);
			std::size_t h2 = std::hash<std::string>{}(k.second);
			return h1 ^ (h2 << 1);
		}
	};

	using CacheMap =
		std::unordered_map<CacheKey,
						std::vector<unsigned char>,
						CacheKeyHash>;

public:
    /// Access the single global instance.
    static PredicateCache& Instance() {
        // Constructed on the first call; guaranteed thread-safe since C++11.
		static PredicateCache cache_instance;
        return cache_instance;
    }

    // Non-copyable / non-movable: prevents additional instances.
    PredicateCache(const PredicateCache&)            = delete;
    PredicateCache& operator=(const PredicateCache&) = delete;
    PredicateCache(PredicateCache&&)                 = delete;
    PredicateCache& operator=(PredicateCache&&)      = delete;

    /*--------------------------------- API ---------------------------------*/

    // Mark the chunk at 'offset' as prunable for (table, filter).
    void Add(const CacheKey& key, Offset offset) {
        std::unique_lock lock(_mutex);      // exclusive write

		auto& vec = _cache[key];
		if (vec.size() <= offset) {
			vec.resize(offset + 1, 0);
		}

		// std::cout << "Vec size: " << vec.size() << ", offset: " << offset << std::endl;
        vec[offset] = 1;
    }

    std::vector<unsigned char>* Get(const CacheKey& key) {
        std::shared_lock lock(_mutex);      // shared read
        auto it = _cache.find(key);
        return (it == _cache.end()) ? nullptr : &it->second;
    }

private:
    PredicateCache()  = default;
    ~PredicateCache() = default;

    CacheMap _cache;
    mutable std::shared_mutex _mutex;
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

protected:
	//! The attached database
	AttachedDatabase &db;
};

} // namespace duckdb
