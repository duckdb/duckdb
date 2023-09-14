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

DUCKDB_API bool IsInvalidSchema(const string &str);
DUCKDB_API bool IsInvalidCatalog(const string &str);

//! Special value used to signify the ROW ID of a table
DUCKDB_API extern const column_t COLUMN_IDENTIFIER_ROW_ID;
DUCKDB_API bool IsRowIdColumnId(column_t column_id);

//! The maximum row identifier used in tables
extern const row_t MAX_ROW_ID;
//! Transaction-local row IDs start at MAX_ROW_ID
extern const row_t MAX_ROW_ID_LOCAL;

extern const transaction_t TRANSACTION_ID_START;
extern const transaction_t MAX_TRANSACTION_ID;
extern const transaction_t MAXIMUM_QUERY_ID;
extern const transaction_t NOT_DELETED_ID;

extern const double PI;

struct DConstants {
	//! The value used to signify an invalid index entry
	static constexpr const idx_t INVALID_INDEX = idx_t(-1);
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
	bool IsValid() {
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
	bool IsValid() {
		return index != DConstants::INVALID_INDEX;
	}
};

DUCKDB_API bool IsPowerOfTwo(uint64_t v);
DUCKDB_API uint64_t NextPowerOfTwo(uint64_t v);
DUCKDB_API uint64_t PreviousPowerOfTwo(uint64_t v);

} // namespace duckdb
