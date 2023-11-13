#ifndef ODBC_INTERVAL_HPP
#define ODBC_INTERVAL_HPP

#include "duckdb_odbc.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/value.hpp"

namespace duckdb {
struct OdbcInterval {
public:
	static bool GetInterval(Value &value, interval_t &interval, OdbcHandleStmt *hstmt);

	static void SetSignal(interval_t &interval, SQL_INTERVAL_STRUCT *interval_struct);

	static void SetYear(interval_t &interval, SQL_INTERVAL_STRUCT *interval_struct);

	static void SetMonth(interval_t &interval, SQL_INTERVAL_STRUCT *interval_struct);

	static void SetDay(interval_t &interval, SQL_INTERVAL_STRUCT *interval_struct);

	static void SetHour(interval_t &interval, SQL_INTERVAL_STRUCT *interval_struct);

	static void SetMinute(interval_t &interval, SQL_INTERVAL_STRUCT *interval_struct);

	static void SetSecond(interval_t &interval, SQL_INTERVAL_STRUCT *interval_struct);

	static void SetDayToHour(interval_t &interval, SQL_INTERVAL_STRUCT *interval_struct);

	static void SetDayToMinute(interval_t &interval, SQL_INTERVAL_STRUCT *interval_struct);

	static void SetDayToSecond(interval_t &interval, SQL_INTERVAL_STRUCT *interval_struct);

	static void SetHourToMinute(interval_t &interval, SQL_INTERVAL_STRUCT *interval_struct);

	static void SetHourToSecond(interval_t &interval, SQL_INTERVAL_STRUCT *interval_struct);

	static void SetMinuteToSecond(interval_t &interval, SQL_INTERVAL_STRUCT *interval_struct);

	static bool IsIntervalType(SQLSMALLINT value_type);

	static SQLSMALLINT GetSQLIntervalType(SQLSMALLINT value_type);

	static SQLSMALLINT GetIntervalCode(SQLSMALLINT value_type);
};
} // namespace duckdb
#endif
