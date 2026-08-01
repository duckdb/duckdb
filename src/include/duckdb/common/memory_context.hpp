//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/memory_context.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/hugeint.hpp"
#include "duckdb/common/string.hpp"

namespace duckdb {

//! Identifies an owner within a shared memory-management domain.
class MemoryContextId {
public:
	constexpr explicit MemoryContextId(hugeint_t uuid_p) : uuid(uuid_p) {
	}

	hugeint_t GetUUID() const {
		return uuid;
	}

	string ToString() const;

	bool operator==(const MemoryContextId &other) const {
		return uuid == other.uuid;
	}

	bool operator!=(const MemoryContextId &other) const {
		return uuid != other.uuid;
	}

private:
	hugeint_t uuid;
};

} // namespace duckdb
