//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/transaction/checkpoint_lock.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"
#include "duckdb/common/shared_ptr.hpp"

namespace duckdb {
struct CheckpointLockInternals;

enum class CheckpointLockType { SHARED, EXCLUSIVE };

class CheckpointLockCoordinator;

class CheckpointLockKey {
public:
	CheckpointLockKey(shared_ptr<CheckpointLockInternals> internals, CheckpointLockType type);
	~CheckpointLockKey();

	CheckpointLockType GetType() const {
		return type;
	}

private:
	shared_ptr<CheckpointLockInternals> internals;
	CheckpointLockType type;

	friend struct CheckpointLockInternals;
	friend class CheckpointLockCoordinator;
};

//! Coordinates transactions that prevent checkpointing with checkpoint operations.
class CheckpointLockCoordinator {
public:
	CheckpointLockCoordinator();
	~CheckpointLockCoordinator();

	unique_ptr<CheckpointLockKey> GetSharedLock();
	unique_ptr<CheckpointLockKey> TryGetExclusiveLock();
	//! Obtain exclusive ownership if the supplied key is the only active shared key.
	//! On success, both the shared key and the returned exclusive key remain active.
	unique_ptr<CheckpointLockKey> TryUpgrade(CheckpointLockKey &lock);

private:
	shared_ptr<CheckpointLockInternals> internals;
};

} // namespace duckdb
