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
	constexpr explicit MemoryContextId(idx_t id_p) : id(id_p) {
	}

	idx_t GetIndex() const {
		return id;
	}

	string ToString() const {
		return StringUtil::Format("%llu", id);
	}

	bool operator==(const MemoryContextId &other) const {
		return id == other.id;
	}

	bool operator!=(const MemoryContextId &other) const {
		return id != other.id;
	}

private:
	idx_t id;
};

} // namespace duckdb
