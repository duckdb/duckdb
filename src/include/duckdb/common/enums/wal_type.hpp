//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/wal_type.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"

namespace duckdb {

enum class WALType : uint8_t {
	INVALID = 0,
	// -----------------------------
	// Catalog
	// -----------------------------
	CREATE_TABLE = 1,
	DROP_TABLE = 2,

	CREATE_SCHEMA = 3,
	DROP_SCHEMA = 4,

	CREATE_VIEW = 5,
	DROP_VIEW = 6,

	CREATE_SEQUENCE = 8,
	DROP_SEQUENCE = 9,
	SEQUENCE_VALUE = 10,

	ALTER_INFO = 20,
	// -----------------------------
	// Data
	// -----------------------------
	USE_TABLE = 25,
	INSERT_TUPLE = 26,
	DELETE_TUPLE = 27,
	UPDATE_TUPLE = 28,
	// -----------------------------
	// Flush
	// -----------------------------
	WAL_FLUSH = 100
};
}
