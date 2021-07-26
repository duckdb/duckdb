#include "odbc_interval.hpp"
#include "duckdb/common/operator/cast_operators.hpp"

using duckdb::interval_t;
using duckdb::OdbcInterval;
using duckdb::Value;

bool OdbcInterval::GetInterval(Value &value, interval_t &interval, duckdb::OdbcHandleStmt *stmt) {
	switch (value.type().id()) {
	case LogicalTypeId::INTERVAL:
		// interval = value.GetValue<interval_t>(); // we don't have a get to retrieve interval
		interval = value.value_.interval;
		return true;
	case LogicalTypeId::VARCHAR: {
		string error_message;
		string val_str = value.GetValue<string>();
		if (!TryCastErrorMessage::Operation<string_t, interval_t>(string_t(val_str), interval, &error_message)) {
			stmt->error_messages.emplace_back(error_message);
			return false;
		}
		return true;
	}
	default:
		return false;
	}
}

/**
 * Set the interval signal, give preference to the most precedent (i.e., year, month, day)
 * */
void OdbcInterval::SetSignal(interval_t &interval, SQL_INTERVAL_STRUCT *interval_struct) {
	if (interval.months != 0) {
		interval_struct->interval_sign = (interval.months < 0) ? SQL_TRUE : SQL_FALSE;
		return;
	}
	if (interval.days != 0) {
		interval_struct->interval_sign = (interval.days < 0) ? SQL_TRUE : SQL_FALSE;
		return;
	}

	interval_struct->interval_sign = (interval.micros < 0) ? SQL_TRUE : SQL_FALSE;
}

void OdbcInterval::SetYear(interval_t &interval, SQL_INTERVAL_STRUCT *interval_struct) {
	interval_struct->interval_type = SQLINTERVAL::SQL_IS_YEAR;
	interval_struct->intval.year_month.year = std::abs(interval.months) / duckdb::Interval::MONTHS_PER_YEAR;
}

void OdbcInterval::SetMonth(interval_t &interval, SQL_INTERVAL_STRUCT *interval_struct) {
	interval_struct->interval_type = SQLINTERVAL::SQL_IS_MONTH;
	interval_struct->intval.year_month.month = std::abs(interval.months);
}

void OdbcInterval::SetDay(interval_t &interval, SQL_INTERVAL_STRUCT *interval_struct) {
	interval_struct->interval_type = SQLINTERVAL::SQL_IS_DAY;
	// set the absolute value of days
	interval_struct->intval.day_second.day =
	    std::abs(interval.days + interval.months * duckdb::Interval::DAYS_PER_MONTH);
}

void OdbcInterval::SetHour(interval_t &interval, SQL_INTERVAL_STRUCT *interval_struct) {
	// firstly, set number of days
	SetDay(interval, interval_struct);
	interval_struct->interval_type = SQLINTERVAL::SQL_IS_HOUR;

	interval_struct->intval.day_second.hour = interval_struct->intval.day_second.day * duckdb::Interval::HOURS_PER_DAY;
	interval_struct->intval.day_second.hour += std::abs(interval.micros) / duckdb::Interval::MICROS_PER_HOUR;
	// remaning stores into the fraction
	interval_struct->intval.day_second.fraction = std::abs(interval.micros) % duckdb::Interval::MICROS_PER_HOUR;
}

void OdbcInterval::SetMinute(interval_t &interval, SQL_INTERVAL_STRUCT *interval_struct) {
	// firstly, set number of hours
	SetHour(interval, interval_struct);
	interval_struct->interval_type = SQLINTERVAL::SQL_IS_MINUTE;

	interval_struct->intval.day_second.minute =
	    interval_struct->intval.day_second.hour * duckdb::Interval::MINS_PER_HOUR;
	interval_struct->intval.day_second.minute +=
	    interval_struct->intval.day_second.fraction / duckdb::Interval::MICROS_PER_MINUTE;
	// remaning stores into the fraction
	interval_struct->intval.day_second.fraction =
	    interval_struct->intval.day_second.fraction % duckdb::Interval::MICROS_PER_MINUTE;
}

void OdbcInterval::SetSecond(interval_t &interval, SQL_INTERVAL_STRUCT *interval_struct) {
	// firstly, set number of minutes
	SetMinute(interval, interval_struct);
	interval_struct->interval_type = SQLINTERVAL::SQL_IS_SECOND;

	interval_struct->intval.day_second.second =
	    interval_struct->intval.day_second.minute * duckdb::Interval::SECS_PER_MINUTE;
	interval_struct->intval.day_second.fraction += std::abs(interval.micros) / duckdb::Interval::MICROS_PER_SEC;
	// remaning stores into the fraction
	interval_struct->intval.day_second.fraction = std::abs(interval.micros) % duckdb::Interval::MICROS_PER_SEC;
}

void OdbcInterval::SetDayToHour(interval_t &interval, SQL_INTERVAL_STRUCT *interval_struct) {
	SetDay(interval, interval_struct);
	interval_struct->interval_type = SQLINTERVAL::SQL_IS_DAY_TO_HOUR;
	// set hours
	interval_struct->intval.day_second.hour = std::abs(interval.micros) / duckdb::Interval::MICROS_PER_HOUR;

	// remaning stores into the fraction
	interval_struct->intval.day_second.fraction = std::abs(interval.micros) % duckdb::Interval::MICROS_PER_HOUR;
}

void OdbcInterval::SetDayToMinute(interval_t &interval, SQL_INTERVAL_STRUCT *interval_struct) {
	SetDayToHour(interval, interval_struct);
	interval_struct->interval_type = SQLINTERVAL::SQL_IS_DAY_TO_MINUTE;
	// set minutes
	interval_struct->intval.day_second.minute =
	    interval_struct->intval.day_second.fraction / duckdb::Interval::MICROS_PER_MINUTE;

	// remaning stores into the fraction
	interval_struct->intval.day_second.fraction =
	    interval_struct->intval.day_second.fraction % duckdb::Interval::MICROS_PER_MINUTE;
}

void OdbcInterval::SetDayToSecond(interval_t &interval, SQL_INTERVAL_STRUCT *interval_struct) {
	SetDayToMinute(interval, interval_struct);
	interval_struct->interval_type = SQLINTERVAL::SQL_IS_DAY_TO_SECOND;
	// set minutes
	interval_struct->intval.day_second.second =
	    interval_struct->intval.day_second.fraction / duckdb::Interval::MICROS_PER_SEC;

	// remaning stores into the fraction
	interval_struct->intval.day_second.fraction =
	    interval_struct->intval.day_second.fraction % duckdb::Interval::MICROS_PER_SEC;
}

void OdbcInterval::SetHourToMinute(interval_t &interval, SQL_INTERVAL_STRUCT *interval_struct) {
	SetHour(interval, interval_struct);
	interval_struct->interval_type = SQLINTERVAL::SQL_IS_HOUR_TO_MINUTE;

	// set minutes
	interval_struct->intval.day_second.minute =
	    interval_struct->intval.day_second.fraction / duckdb::Interval::MICROS_PER_MINUTE;

	// remaning stores into the fraction
	interval_struct->intval.day_second.fraction =
	    interval_struct->intval.day_second.fraction % duckdb::Interval::MICROS_PER_MINUTE;
}

void OdbcInterval::SetHourToSecond(interval_t &interval, SQL_INTERVAL_STRUCT *interval_struct) {
	SetHourToMinute(interval, interval_struct);
	interval_struct->interval_type = SQLINTERVAL::SQL_IS_HOUR_TO_SECOND;

	// set seconds
	interval_struct->intval.day_second.second =
	    interval_struct->intval.day_second.fraction / duckdb::Interval::MICROS_PER_SEC;

	// remaning stores into the fraction
	interval_struct->intval.day_second.fraction =
	    interval_struct->intval.day_second.fraction % duckdb::Interval::MICROS_PER_SEC;
}

void OdbcInterval::SetMinuteToSecond(interval_t &interval, SQL_INTERVAL_STRUCT *interval_struct) {
	SetMinute(interval, interval_struct);
	interval_struct->interval_type = SQLINTERVAL::SQL_IS_MINUTE_TO_SECOND;

	// set seconds
	interval_struct->intval.day_second.second =
	    interval_struct->intval.day_second.fraction / duckdb::Interval::MICROS_PER_SEC;

	// remaning stores into the fraction
	interval_struct->intval.day_second.fraction =
	    interval_struct->intval.day_second.fraction % duckdb::Interval::MICROS_PER_SEC;
}