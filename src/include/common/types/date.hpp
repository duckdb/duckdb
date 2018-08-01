
#pragma once

#include <string>

#include "common/internal_types.hpp"
#include "common/printable.hpp"

namespace duckdb {

typedef int32_t date_t;

class Date {
  public:
	static date_t FromString(std::string str);
	static std::string ToString(date_t date);

	static std::string Format(int32_t year, int32_t month, int32_t day);

	static void Convert(date_t date, int32_t &out_year, int32_t &out_month,
	                    int32_t &out_day);
	static date_t FromDate(int32_t year, int32_t month, int32_t day);

	static bool IsLeapYear(int32_t year);
	static bool IsValidDay(int32_t year, int32_t month, int32_t day);
};
} // namespace duckdb
