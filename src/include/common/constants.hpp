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

//! The vector size used in the execution engine
#define STANDARD_VECTOR_SIZE 1024
//! The amount of vectors per storage chunk
#define STORAGE_CHUNK_VECTORS 10
//! The storage chunk size
#define STORAGE_CHUNK_SIZE (STANDARD_VECTOR_SIZE * STORAGE_CHUNK_VECTORS)

//! a saner size_t for loop indices etc
typedef uint64_t index_t;

//! The type used for row identifiers
typedef int64_t row_t;

//! The value used to signify an invalid index entry
extern const index_t INVALID_INDEX;

//! data pointers
typedef uint8_t data_t;
typedef data_t *data_ptr_t;
typedef const data_t *const_data_ptr_t;

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
//! Special value used to signify the ROW ID of a table
extern const column_t COLUMN_IDENTIFIER_ROW_ID;

//! Zero selection vector: completely filled with the value 0 [READ ONLY]
extern const sel_t ZERO_VECTOR[STANDARD_VECTOR_SIZE];

extern const double PI;

} // namespace duckdb
