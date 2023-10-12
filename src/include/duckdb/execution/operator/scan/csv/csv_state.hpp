//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/scan/csv/csv_state.hpp
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
	EMPTY_LINE = 7,       //! State when encountering an empty line (i.e., \r\r \n\n, \n\r)
	INVALID = 8           //! Got to an Invalid State, this should error.
};

} // namespace duckdb
