//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/constants.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include "duckdb/common/string.hpp"
#include "duckdb/common/winapi.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/typedefs.hpp"

namespace duckdb {
class Serializer;
class Deserializer;
class BinarySerializer;
class BinaryDeserializer;
class WriteStream;
class ReadStream;

//! inline std directives that we use frequently
#ifndef DUCKDB_DEBUG_MOVE
using std::move;
#endif

// NOTE: there is a copy of this in the Postgres' parser grammar (gram.y)
#define DEFAULT_SCHEMA  "main"
#define INVALID_SCHEMA  ""
#define INVALID_CATALOG ""
#define SYSTEM_CATALOG  "system"
#define TEMP_CATALOG    "temp"
#define IN_MEMORY_PATH  ":memory:"

DUCKDB_API bool IsInvalidSchema(const string &str);
DUCKDB_API bool IsInvalidCatalog(const string &str);

//! Special value used to signify the ROW ID of a table
DUCKDB_API extern const column_t COLUMN_IDENTIFIER_ROW_ID;
//! Special value used to signify an empty column (used for e.g. COUNT(*))
DUCKDB_API extern const column_t COLUMN_IDENTIFIER_EMPTY;
//! Special value used to signify the ROW_NUMBER of a table
DUCKDB_API extern const column_t COLUMN_IDENTIFIER_ROW_NUMBER;
DUCKDB_API extern const column_t VIRTUAL_COLUMN_START;
DUCKDB_API bool IsRowIdColumnId(column_t column_id);
DUCKDB_API bool IsRowNumberColumnId(column_t column_id);
DUCKDB_API bool IsVirtualColumn(column_t column_id);

//! The maximum row identifier used in tables
extern const row_t MAX_ROW_ID;
//! Transaction-local row IDs start at MAX_ROW_ID
extern const row_t MAX_ROW_ID_LOCAL;

extern const transaction_t SYSTEM_TRANSACTION_TIMESTAMP;
extern const transaction_t TRANSACTION_ID_START;
//! The largest timestamp that is still a commit id, one below the first transaction id
extern const transaction_t MAX_COMMIT_ID;
extern const transaction_t MAX_TRANSACTION_ID;
extern const transaction_t MAXIMUM_QUERY_ID;
extern const transaction_t NOT_DELETED_ID;

//! An exclusive bound on the transaction timeline: timestamps below it are visible. Its own type, so
//! it cannot be swapped or assigned with a timestamp; compare with < and >= only.
struct VisibilityBound {
	VisibilityBound() = default;

	//! Visible up to and including this timestamp
	static VisibilityBound Through(transaction_t timestamp) {
		return VisibilityBound(timestamp + 1);
	}
	//! Visible strictly before this timestamp
	static VisibilityBound Before(transaction_t timestamp) {
		return VisibilityBound(timestamp);
	}
	static VisibilityBound AllCommitted() {
		return Through(MAX_COMMIT_ID);
	}
	static VisibilityBound IncludingUncommitted() {
		return VisibilityBound(MAX_TRANSACTION_ID);
	}
	static VisibilityBound Min(VisibilityBound a, VisibilityBound b) {
		return a.value < b.value ? a : b;
	}

private:
	explicit VisibilityBound(transaction_t value_p) : value(value_p) {
	}

	friend bool operator==(VisibilityBound a, VisibilityBound b);
	friend bool operator!=(VisibilityBound a, VisibilityBound b);
	friend bool operator<(transaction_t timestamp, VisibilityBound bound);
	friend bool operator>=(transaction_t timestamp, VisibilityBound bound);

	transaction_t value = 0;
};

inline bool operator==(VisibilityBound a, VisibilityBound b) {
	return a.value == b.value;
}
inline bool operator!=(VisibilityBound a, VisibilityBound b) {
	return a.value != b.value;
}

//! Only < and >= exist; <= and > do not compile, so a bound stays exclusive.
inline bool operator<(transaction_t timestamp, VisibilityBound bound) {
	return timestamp < bound.value;
}
inline bool operator>=(transaction_t timestamp, VisibilityBound bound) {
	return !(timestamp < bound);
}

extern const double PI;

struct DConstants {
	//! The value used to signify an invalid index entry
	static constexpr const idx_t INVALID_INDEX = idx_t(-1);
	//! The total maximum vector size (128GB)
	static constexpr const idx_t MAX_VECTOR_SIZE = 1ULL << 37ULL;
};

struct LogicalIndex {
	explicit LogicalIndex(idx_t index) : index(index) {
	}

	idx_t index;

	inline bool operator==(const LogicalIndex &rhs) const {
		return index == rhs.index;
	};
	inline bool operator!=(const LogicalIndex &rhs) const {
		return index != rhs.index;
	};
	inline bool operator<(const LogicalIndex &rhs) const {
		return index < rhs.index;
	};
	bool IsValid() const {
		return index != DConstants::INVALID_INDEX;
	}
};

struct PhysicalIndex {
	explicit PhysicalIndex(idx_t index) : index(index) {
	}

	idx_t index;

	inline bool operator==(const PhysicalIndex &rhs) const {
		return index == rhs.index;
	};
	inline bool operator!=(const PhysicalIndex &rhs) const {
		return index != rhs.index;
	};
	inline bool operator<(const PhysicalIndex &rhs) const {
		return index < rhs.index;
	};
	bool IsValid() const {
		return index != DConstants::INVALID_INDEX;
	}
};

DUCKDB_API bool IsPowerOfTwo(uint64_t v);
DUCKDB_API uint64_t NextPowerOfTwo(uint64_t v);
DUCKDB_API uint64_t PreviousPowerOfTwo(uint64_t v);

} // namespace duckdb
