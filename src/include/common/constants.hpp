//===----------------------------------------------------------------------===//
//                         DuckDB
//
// common/constants.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstdlib>
#include <memory>
#include <string>
#include <vector>

namespace duckdb {

//! inline std directives that we use frequently
using std::move;
using std::string;
using std::unique_ptr;
using data_ptr = unique_ptr<char[]>;
using std::vector;

// NOTE: there is a copy of this in the Postgres' parser grammar (gram.y)
#define DEFAULT_SCHEMA "main"

#define STANDARD_VECTOR_SIZE 1024
#define STORAGE_CHUNK_SIZE 10240

typedef uint64_t index_t;
const index_t INVALID_INDEX = (index_t)-1;

typedef uint8_t *data_t;
typedef const uint8_t *data_t_const;

//! Type used to represent dates
typedef int32_t date_t;
//! Type used to represent time
typedef int32_t dtime_t;
//! Type used to represent timestamps
typedef int64_t timestamp_t;
//! Type used for the selection vector
typedef uint16_t sel_t;
//! Type used for transaction timestamps
//! FIXME: this should be a 128-bit integer
//! With 64-bit, the database only supports up to 2^32 transactions
typedef index_t transaction_t;

//! Type used for column identifiers
typedef index_t column_t;
//! Special value used to signify the ROW ID of
extern column_t COLUMN_IDENTIFIER_ROW_ID;

//! Zero selection vector: completely filled with the value 0 [READ ONLY]
extern sel_t ZERO_VECTOR[STANDARD_VECTOR_SIZE];

} // namespace duckdb
