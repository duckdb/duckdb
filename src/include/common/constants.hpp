//===----------------------------------------------------------------------===//
//                         DuckDB
//
// common/constants.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <bitset>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

namespace duckdb {

//! inline std directives that we use frequently
using std::fstream;
using std::ios;
using std::ios_base;
using std::map;
using std::move;
using std::string;
using std::unique_ptr;
using std::unordered_map;
using std::vector;
using data_ptr = unique_ptr<char[]>;

struct BinaryData {
	unique_ptr<uint8_t[]> data;
	size_t size;
};

// NOTE: there is a copy of this in the Postgres' parser grammar (gram.y)
#define DEFAULT_SCHEMA "main"

#define STANDARD_VECTOR_SIZE 1024
#define STORAGE_CHUNK_SIZE 10240

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
typedef uint64_t transaction_t;
//! Type used for nullmasks
typedef std::bitset<STANDARD_VECTOR_SIZE> nullmask_t;

//! Type used for column identifiers
typedef size_t column_t;
//! Special value used to signify the ROW ID of
extern column_t COLUMN_IDENTIFIER_ROW_ID;

//! Zero selection vector: completely filled with the value 0 [READ ONLY]
extern sel_t ZERO_VECTOR[STANDARD_VECTOR_SIZE];
//! Zero NULL mask: filled with the value 0 [READ ONLY]
extern nullmask_t ZERO_MASK;

} // namespace duckdb
