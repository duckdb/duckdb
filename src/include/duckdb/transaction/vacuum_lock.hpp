//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/transaction/vacuum_lock.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"
#include "duckdb/common/shared_ptr.hpp"

namespace duckdb {
struct VacuumLockInternals;

enum class VacuumLockType { SHARED, EXCLUSIVE };

class VacuumLockKey {
public:
	VacuumLockKey(shared_ptr<VacuumLockInternals> internals, VacuumLockType type);
	~VacuumLockKey();

	VacuumLockType GetType() const {
		return type;
	}

private:
	shared_ptr<VacuumLockInternals> internals;
	VacuumLockType type;
};

//! Coordinates long-lived leases that prevent full vacuum/checkpoint rewrites.
class VacuumLockCoordinator {
public:
	VacuumLockCoordinator();
	~VacuumLockCoordinator();

	unique_ptr<VacuumLockKey> GetSharedLock();
	unique_ptr<VacuumLockKey> TryGetExclusiveLock();

private:
	shared_ptr<VacuumLockInternals> internals;
};

} // namespace duckdb
