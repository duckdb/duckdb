//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// common/types/date.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>

#include "common/internal_types.hpp"
#include "common/printable.hpp"

namespace duckdb {

//! The Date class is a static class that holds helper functions for the Date
//! type.
class Date {
  public:
	//! Convert a string in the format "YYYY-MM-DD" to a date object
	static date_t FromString(std::string str);
	//! Convert a date object to a string in the format "YYYY-MM-DD"
	static std::string ToString(date_t date);

	//! Create a string "YYYY-MM-DD" from a specified (year, month, day)
	//! combination
	static std::string Format(int32_t year, int32_t month, int32_t day);

	//! Extract the year, month and day from a given date object
	static void Convert(date_t date, int32_t &out_year, int32_t &out_month,
	                    int32_t &out_day);
	//! Create a Date object from a specified (year, month, day) combination
	static date_t FromDate(int32_t year, int32_t month, int32_t day);

	//! Returns true if (year) is a leap year, and false otherwise
	static bool IsLeapYear(int32_t year);

	//! Returns true if the specified (year, month, day) combination is a valid
	//! date
	static bool IsValidDay(int32_t year, int32_t month, int32_t day);
};
} // namespace duckdb
