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
	STANDARD = 0,  //! Regular unquoted field state
	DELIMITER = 1, //! State after encountering a field separator (e.g., ;) - This is always the last delimiter byte
	//! ------------- Multi-byte Delimiter States (up to 4 bytes) ----------------------------------------------!//
	DELIMITER_FIRST_BYTE = 2,  //! State when encountering the first delimiter byte of a multi-byte delimiter
	DELIMITER_SECOND_BYTE = 3, //! State when encountering the second delimiter byte of a multi-byte delimiter
	DELIMITER_THIRD_BYTE = 4,  //! State when encountering the third delimiter byte of a multi-byte delimiter
	//! --------------------------------------------------------------------------------------------------------!//
	RECORD_SEPARATOR = 5,  //! State after encountering a record separator (i.e., \n)
	CARRIAGE_RETURN = 6,   //! State after encountering a carriage return(i.e., \r)
	QUOTED = 7,            //! State when inside a quoted field
	UNQUOTED = 8,          //! State when leaving a quoted field
	ESCAPE = 9,            //! State when encountering an escape character (e.g., \)
	INVALID = 10,          //! Got to an Invalid State, this should error.
	NOT_SET = 11,          //! If the state is not set, usually the first state before getting the first character
	QUOTED_NEW_LINE = 12,  //! If we have a quoted newline
	EMPTY_SPACE = 13,      //! If we have empty spaces in the beginning and end of value
	COMMENT = 14,          //! If we are in a comment state, and hence have to skip the whole line
	STANDARD_NEWLINE = 15, //! State used for figuring out a new line.
	UNQUOTED_ESCAPE = 16,  //! State when encountering an escape character (e.g., \) in an unquoted field
	ESCAPED_RETURN = 17, //! State when the carriage return is preceded by an escape character (for '\r\n' newline only)
	MAYBE_QUOTED = 18    //! We are potentially in a quoted value or the end of an unquoted value
};
} // namespace duckdb
