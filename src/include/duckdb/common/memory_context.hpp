//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/memory_context.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

//! Identifies an owner within a shared memory-management domain.
class MemoryContextId {
public:
	constexpr explicit MemoryContextId(idx_t database_id_p) : database_id(database_id_p) {
	}

	idx_t GetDatabaseId() const {
		return database_id;
	}

	string ToString() const {
		return StringUtil::Format("%llu", database_id);
	}

	bool operator==(const MemoryContextId &other) const {
		return database_id == other.database_id;
	}

	bool operator!=(const MemoryContextId &other) const {
		return database_id != other.database_id;
	}

private:
	idx_t database_id;
};

} // namespace duckdb
