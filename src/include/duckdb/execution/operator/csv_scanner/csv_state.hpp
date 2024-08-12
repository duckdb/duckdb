//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/csv_scanner/csv_state.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstdint>

namespace duckdb {

//! All States of CSV Parsing
enum class CSVState : uint8_t {
	STANDARD = 0,         //! Regular unquoted field state
	DELIMITER = 1,        //! State after encountering a field separator (e.g., ;)
	RECORD_SEPARATOR = 2, //! State after encountering a record separator (i.e., \n)
	CARRIAGE_RETURN = 3,  //! State after encountering a carriage return(i.e., \r)
	QUOTED = 4,           //! State when inside a quoted field
	UNQUOTED = 5,         //! State when leaving a quoted field
	ESCAPE = 6,           //! State when encountering an escape character (e.g., \)
	INVALID = 7,          //! Got to an Invalid State, this should error.
	NOT_SET = 8,          //! If the state is not set, usually the first state before getting the first character
	QUOTED_NEW_LINE = 9,  //! If we have a quoted newline
	EMPTY_SPACE = 10,     //! If we have empty spaces in the beginning and end of value
	COMMENT = 11          //! If we are in a comment state, and hence have to skip the whole line
};

} // namespace duckdb
