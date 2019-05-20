//===----------------------------------------------------------------------===//
//                         DuckDB
//
// common/enums/wal_type.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/constants.hpp"

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
	// -----------------------------
	// Data
	// -----------------------------
	INSERT_TUPLE = 25,
	QUERY = 26,
	// -----------------------------
	// Flush
	// -----------------------------
	WAL_FLUSH = 100
};

}
