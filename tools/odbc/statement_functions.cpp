#include "statement_functions.hpp"
#include "odbc_interval.hpp"

#include "duckdb/common/types/decimal.hpp"
#include "duckdb/common/types/string_type.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/types/blob.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/timestamp.hpp"

#include <algorithm>

using std::string;

using duckdb::date_t;
using duckdb::Decimal;
using duckdb::DecimalType;
using duckdb::dtime_t;
using duckdb::hugeint_t;
using duckdb::interval_t;
using duckdb::LogicalType;
using duckdb::LogicalTypeId;
using duckdb::OdbcInterval;
using duckdb::Store;
using duckdb::string_t;
using duckdb::timestamp_t;

SQLRETURN duckdb::PrepareStmt(SQLHSTMT statement_handle, SQLCHAR *statement_text, SQLINTEGER text_length) {
	return duckdb::WithStatement(statement_handle, [&](duckdb::OdbcHandleStmt *stmt) {
		if (stmt->stmt) {
			stmt->stmt.reset();
		}
		if (stmt->res) {
			stmt->res.reset();
		}
		if (stmt->chunk) {
			stmt->chunk.reset();
		}
		// we should not clear the parameters because of SQLExecDirect may reuse them
		// stmt->params.resize(0);
		// we should not clear the bound columns because SQLBindCol might bind columns in it
		// stmt->bound_cols.resize(0);

		auto query = duckdb::OdbcUtils::ReadString(statement_text, text_length);
		stmt->stmt = stmt->dbc->conn->Prepare(query);
		if (!stmt->stmt->success) {
			stmt->error_messages.emplace_back(stmt->stmt->error);
			return SQL_ERROR;
		}
		stmt->params.resize(stmt->stmt->n_param);
		stmt->bound_cols.resize(stmt->stmt->ColumnCount());
		return SQL_SUCCESS;
	});
}

SQLRETURN duckdb::ExecuteStmt(SQLHSTMT statement_handle) {
	return duckdb::WithStatement(statement_handle, [&](duckdb::OdbcHandleStmt *stmt) {
		if (stmt->res) {
			stmt->res.reset();
		}
		if (stmt->chunk) {
			stmt->chunk.reset();
		}

		stmt->chunk_row = -1;
		stmt->open = false;
		if (stmt->rows_fetched_ptr) {
			*stmt->rows_fetched_ptr = 0;
		}
		stmt->res = stmt->stmt->Execute(stmt->params);
		if (!stmt->res->success) {
			stmt->error_messages.emplace_back(stmt->res->error);
			return SQL_ERROR;
		}
		stmt->open = true;
		return SQL_SUCCESS;
	});
}

//! Static fuctions used by GetDataStmtResult //

static bool ValidateType(LogicalTypeId input, LogicalTypeId expected, duckdb::OdbcHandleStmt *stmt) {
	if (input != expected) {
		stmt->error_messages.emplace_back("Type mismatch error: received " + LogicalTypeIdToString(input) +
		                                  ", but expected " + LogicalTypeIdToString(expected));
		return false;
	}
	return true;
}

static void LogInvalidCast(const LogicalType &from_type, const LogicalType &to_type, duckdb::OdbcHandleStmt *stmt) {
	string msg = "Not implemented Error: Unimplemented type for cast (" + from_type.ToString() + " -> " +
	             to_type.ToString() + ")";
	stmt->error_messages.emplace_back(msg);
}

template <class T>
static SQLRETURN GetInternalValue(duckdb::OdbcHandleStmt *stmt, const duckdb::Value &val, const LogicalType &type,
                                  SQLLEN buffer_length, SQLPOINTER target_value_ptr) {
	D_ASSERT(((size_t)buffer_length) >= sizeof(T));
	try {
		auto casted_value = val.CastAs(type).GetValue<T>();
		Store<T>(casted_value, (duckdb::data_ptr_t)target_value_ptr);
		return SQL_SUCCESS;
	} catch (std::exception &ex) {
		stmt->error_messages.emplace_back(ex.what());
		return SQL_ERROR;
	}
}

template <class CAST_OP, typename TARGET_TYPE, class CAST_FUNC = std::function<timestamp_t(int64_t)>>
static bool CastTimestampValue(duckdb::OdbcHandleStmt *stmt, const duckdb::Value &val, TARGET_TYPE &target,
                               CAST_FUNC cast_timestamp_fun) {
	try {
		timestamp_t timestamp = cast_timestamp_fun(val.GetValue<int64_t>());
		target = CAST_OP::template Operation<timestamp_t, TARGET_TYPE>(timestamp);
		return true;
	} catch (duckdb::Exception &ex) {
		stmt->error_messages.emplace_back(ex.what());
		return false;
	}
}

SQLRETURN duckdb::GetDataStmtResult(SQLHSTMT statement_handle, SQLUSMALLINT col_or_param_num, SQLSMALLINT target_type,
                                    SQLPOINTER target_value_ptr, SQLLEN buffer_length, SQLLEN *str_len_or_ind_ptr) {

	return duckdb::WithStatementResult(statement_handle, [&](duckdb::OdbcHandleStmt *stmt) -> SQLRETURN {
		if (!target_value_ptr) {
			return SQL_ERROR;
		}

		if (!stmt->chunk) {
			return SQL_ERROR;
		}
		auto val = stmt->chunk->GetValue(col_or_param_num - 1, stmt->chunk_row);
		if (val.is_null) {
			if (!str_len_or_ind_ptr) {
				return SQL_ERROR;
			}
			*str_len_or_ind_ptr = SQL_NULL_DATA;
			return SQL_SUCCESS;
		}

		switch (target_type) {
		case SQL_C_SSHORT:
			return GetInternalValue<SQLSMALLINT>(stmt, val, LogicalType::SMALLINT, buffer_length, target_value_ptr);
		case SQL_C_USHORT:
			return GetInternalValue<SQLUSMALLINT>(stmt, val, LogicalType::USMALLINT, buffer_length, target_value_ptr);
		case SQL_C_LONG:
		case SQL_C_SLONG:
			return GetInternalValue<SQLINTEGER>(stmt, val, LogicalType::INTEGER, buffer_length, target_value_ptr);
		case SQL_C_ULONG:
			return GetInternalValue<SQLUINTEGER>(stmt, val, LogicalType::UINTEGER, buffer_length, target_value_ptr);
		case SQL_C_FLOAT:
			return GetInternalValue<SQLREAL>(stmt, val, LogicalType::FLOAT, buffer_length, target_value_ptr);
		case SQL_C_DOUBLE:
			return GetInternalValue<SQLFLOAT>(stmt, val, LogicalType::DOUBLE, buffer_length, target_value_ptr);
		case SQL_C_BIT: {
			LogicalType char_type = LogicalType(LogicalTypeId::CHAR);
			return GetInternalValue<SQLCHAR>(stmt, val, char_type, buffer_length, target_value_ptr);
		}
		case SQL_C_STINYINT:
			return GetInternalValue<SQLSCHAR>(stmt, val, LogicalType::TINYINT, buffer_length, target_value_ptr);
		case SQL_C_UTINYINT:
			return GetInternalValue<SQLSCHAR>(stmt, val, LogicalType::UTINYINT, buffer_length, target_value_ptr);
		case SQL_C_SBIGINT:
			return GetInternalValue<SQLBIGINT>(stmt, val, LogicalType::BIGINT, buffer_length, target_value_ptr);
		case SQL_C_UBIGINT:
			// case SQL_C_BOOKMARK: // same ODBC type (\\TODO we don't support bookmark types)
			return GetInternalValue<SQLUBIGINT>(stmt, val, LogicalType::UBIGINT, buffer_length, target_value_ptr);
		case SQL_C_WCHAR: {
			std::string str = val.GetValue<std::string>();
			std::wstring w_str = std::wstring(str.begin(), str.end());
			auto out_len = swprintf((wchar_t *)target_value_ptr, buffer_length, L"%ls", w_str.c_str());

			if (str_len_or_ind_ptr) {
				*str_len_or_ind_ptr = out_len;
			}
			return SQL_SUCCESS;
		}
		// case SQL_C_VARBOOKMARK: // same ODBC type (\\TODO we don't support bookmark types)
		case SQL_C_BINARY: {
			// threating binary values as BLOB type
			string blob = duckdb::Blob::ToBlob(duckdb::string_t(val.GetValue<string>().c_str()));
			auto out_len = snprintf((char *)target_value_ptr, buffer_length, "%s", blob.c_str());

			if (str_len_or_ind_ptr) {
				*str_len_or_ind_ptr = out_len;
			}
			return SQL_SUCCESS;
		}
		case SQL_C_CHAR: {
			auto out_len = snprintf((char *)target_value_ptr, buffer_length, "%s", val.GetValue<string>().c_str());

			if (str_len_or_ind_ptr) {
				*str_len_or_ind_ptr = out_len;
			}
			return SQL_SUCCESS;
		}
		case SQL_C_NUMERIC: {
			if (!ValidateType(val.type().id(), LogicalTypeId::DECIMAL, stmt)) {
				return SQL_ERROR;
			}
			SQL_NUMERIC_STRUCT *numeric = (SQL_NUMERIC_STRUCT *)target_value_ptr;
			auto dataptr = (duckdb::data_ptr_t)numeric->val;
			// reset numeric val to remove some garbage
			memset(dataptr, '\0', SQL_MAX_NUMERIC_LEN);

			numeric->sign = 1;
			numeric->precision = numeric->scale = 0;

			string str_val = val.ToString();
			auto width = str_val.size();

			if (str_val[0] == '-') {
				numeric->sign = 0;
				str_val.erase(std::remove(str_val.begin(), str_val.end(), '-'), str_val.end());
				// uncounting negative signal '-'
				--width;
			}

			auto pos_dot = str_val.find('.');
			if (pos_dot != string::npos) {
				str_val.erase(std::remove(str_val.begin(), str_val.end(), '.'), str_val.end());
				numeric->scale = str_val.size() - pos_dot;

				string str_fraction = str_val.substr(pos_dot);
				// case all digits in fraction is 0, remove them
				if (std::stoi(str_fraction) == 0) {
					str_val.erase(str_val.begin() + pos_dot, str_val.end());
				}
				width = str_val.size();
			}
			numeric->precision = width;

			string_t str_t(str_val.c_str(), width);
			if (numeric->precision <= Decimal::MAX_WIDTH_INT64) {
				int64_t val_i64;
				duckdb::TryCast::Operation(str_t, val_i64);
				memcpy(dataptr, &val_i64, sizeof(val_i64));
			} else {
				hugeint_t huge_int =
				    duckdb::CastToDecimal::Operation<string_t, hugeint_t>(str_t, numeric->precision, numeric->scale);
				memcpy(dataptr, &huge_int.lower, sizeof(huge_int.lower));
				memcpy(dataptr + sizeof(huge_int.lower), &huge_int.upper, sizeof(huge_int.upper));
			}
			return SQL_SUCCESS;
		}
		case SQL_C_TYPE_DATE: {
			date_t date;
			switch (val.type().id()) {
			case LogicalTypeId::DATE:
				date = val.GetValue<date_t>();
				break;
			case LogicalTypeId::TIMESTAMP_SEC: {
				if (!CastTimestampValue<duckdb::Cast, date_t>(stmt, val, date,
				                                                             duckdb::Timestamp::FromEpochSeconds)) {
					return SQL_ERROR;
				}
				break;
			}
			case LogicalTypeId::TIMESTAMP_MS: {
				if (!CastTimestampValue<duckdb::Cast, date_t>(stmt, val, date,
				                                                             duckdb::Timestamp::FromEpochMs)) {
					return SQL_ERROR;
				}
				break;
			}
			case LogicalTypeId::TIMESTAMP: {
				if (!CastTimestampValue<duckdb::Cast, date_t>(
				        stmt, val, date, duckdb::Timestamp::FromEpochMicroSeconds)) {
					return SQL_ERROR;
				}
				break;
			}
			case LogicalTypeId::TIMESTAMP_NS: {
				if (!CastTimestampValue<duckdb::Cast, date_t>(stmt, val, date,
				                                                             duckdb::Timestamp::FromEpochNanoSeconds)) {
					return SQL_ERROR;
				}
				break;
			}
			case LogicalTypeId::VARCHAR: {
				string val_str = val.GetValue<string>();
				try {
					date = duckdb::CastToDate::Operation<string_t, date_t>(string_t(val_str));
				} catch (duckdb::Exception &ex) {
					stmt->error_messages.emplace_back(ex.what());
					return SQL_ERROR;
				}
				break;
			}
			default:
				LogInvalidCast(val.type(), LogicalType::DATE, stmt);
				return SQL_ERROR;
			} // end switch "val.type().id()": SQL_C_TYPE_DATE

			SQL_DATE_STRUCT *date_struct = (SQL_DATE_STRUCT *)target_value_ptr;
			try {
				date_struct->year = duckdb::Date::ExtractYear(date);
				date_struct->month = duckdb::Date::ExtractMonth(date);
				date_struct->day = duckdb::Date::ExtractDay(date);
				return SQL_SUCCESS;
			} catch (duckdb::Exception &ex) {
				stmt->error_messages.emplace_back(ex.what());
				return SQL_ERROR;
			}
		}
		case SQL_C_TYPE_TIME: {
			dtime_t time;
			switch (val.type().id()) {
			case LogicalTypeId::TIME:
				time = val.GetValue<dtime_t>();
				break;
			case LogicalTypeId::TIMESTAMP_SEC: {
				if (!CastTimestampValue<duckdb::CastTimestampToTime, dtime_t>(stmt, val, time,
				                                                              duckdb::Timestamp::FromEpochSeconds)) {
					return SQL_ERROR;
				}
				break;
			}
			case LogicalTypeId::TIMESTAMP_MS: {
				if (!CastTimestampValue<duckdb::CastTimestampToTime, dtime_t>(stmt, val, time,
				                                                              duckdb::Timestamp::FromEpochMs)) {
					return SQL_ERROR;
				}
				break;
			}
			case LogicalTypeId::TIMESTAMP: {
				if (!CastTimestampValue<duckdb::CastTimestampToTime, dtime_t>(
				        stmt, val, time, duckdb::Timestamp::FromEpochMicroSeconds)) {
					return SQL_ERROR;
				}
				break;
			}
			case LogicalTypeId::TIMESTAMP_NS: {
				if (!CastTimestampValue<duckdb::CastTimestampToTime, dtime_t>(
				        stmt, val, time, duckdb::Timestamp::FromEpochNanoSeconds)) {
					return SQL_ERROR;
				}
				break;
			}
			case LogicalTypeId::VARCHAR: {
				string val_str = val.GetValue<string>();
				try {
					time = duckdb::CastToTime::Operation<string_t, dtime_t>(string_t(val_str));
				} catch (duckdb::Exception &ex) {
					stmt->error_messages.emplace_back(ex.what());
					return SQL_ERROR;
				}
				break;
			}
			default:
				LogInvalidCast(val.type(), LogicalType::TIME, stmt);
				return SQL_ERROR;
			} // end switch "val.type().id()": SQL_C_TYPE_TIME

			SQL_TIME_STRUCT *time_struct = (SQL_TIME_STRUCT *)target_value_ptr;
			try {
				int32_t hour, minute, second, micros;
				duckdb::Time::Convert(time, hour, minute, second, micros);

				time_struct->hour = hour;
				time_struct->minute = minute;
				time_struct->second = second;
				return SQL_SUCCESS;
			} catch (duckdb::Exception &ex) {
				stmt->error_messages.emplace_back(ex.what());
				return SQL_ERROR;
			}
		}
		case SQL_C_TYPE_TIMESTAMP: {
			timestamp_t timestamp;
			switch (val.type().id()) {
			case LogicalTypeId::TIMESTAMP_SEC:
				timestamp = duckdb::Timestamp::FromEpochSeconds(val.GetValue<int64_t>());
				break;
			case LogicalTypeId::TIMESTAMP_MS:
				timestamp = duckdb::Timestamp::FromEpochMs(val.GetValue<int64_t>());
				break;
			case LogicalTypeId::TIMESTAMP:
				timestamp = duckdb::Timestamp::FromEpochMicroSeconds(val.GetValue<int64_t>());
				break;
			case LogicalTypeId::TIMESTAMP_NS:
				timestamp = duckdb::Timestamp::FromEpochNanoSeconds(val.GetValue<int64_t>());
				break;
			case LogicalTypeId::DATE: {
				try {
					timestamp = duckdb::CastDateToTimestamp::Operation<date_t, timestamp_t>(val.GetValue<date_t>());
				} catch (duckdb::Exception &ex) {
					stmt->error_messages.emplace_back(ex.what());
					return SQL_ERROR;
				}
				break;
			}
			case LogicalTypeId::VARCHAR: {
				string val_str = val.GetValue<string>();
				try {
					timestamp = duckdb::CastToTimestamp::Operation<string_t, timestamp_t>(string_t(val_str));
				} catch (duckdb::Exception &ex) {
					stmt->error_messages.emplace_back(ex.what());
					return SQL_ERROR;
				}
				break;
			}
			default:
				LogInvalidCast(val.type(), LogicalType::TIMESTAMP, stmt);
				return SQL_ERROR;
			} // end switch "val.type().id()"

			SQL_TIMESTAMP_STRUCT *timestamp_struct = (SQL_TIMESTAMP_STRUCT *)target_value_ptr;
			try {
				date_t date = duckdb::Timestamp::GetDate(timestamp);
				timestamp_struct->year = duckdb::Date::ExtractYear(date);
				timestamp_struct->month = duckdb::Date::ExtractMonth(date);
				timestamp_struct->day = duckdb::Date::ExtractDay(date);

				dtime_t time = duckdb::Timestamp::GetTime(timestamp);
				int32_t hour, minute, second, micros;
				duckdb::Time::Convert(time, hour, minute, second, micros);
				timestamp_struct->hour = hour;
				timestamp_struct->minute = minute;
				timestamp_struct->second = second;
				timestamp_struct->fraction = micros;

				return SQL_SUCCESS;
			} catch (duckdb::Exception &ex) {
				stmt->error_messages.emplace_back(ex.what());
				return SQL_ERROR;
			}
		}
		case SQL_C_INTERVAL_YEAR: {
			interval_t interval;
			if (!OdbcInterval::GetInterval(val, interval, stmt)) {
				LogInvalidCast(val.type(), LogicalType::INTERVAL, stmt);
				return SQL_ERROR;
			}

			SQL_INTERVAL_STRUCT *interval_struct = (SQL_INTERVAL_STRUCT *)target_value_ptr;
			OdbcInterval::SetYear(interval, interval_struct);
			OdbcInterval::SetSignal(interval, interval_struct);
			return SQL_SUCCESS;
		}
		case SQL_C_INTERVAL_MONTH: {
			interval_t interval;
			if (!OdbcInterval::GetInterval(val, interval, stmt)) {
				LogInvalidCast(val.type(), LogicalType::INTERVAL, stmt);
				return SQL_ERROR;
			}

			SQL_INTERVAL_STRUCT *interval_struct = (SQL_INTERVAL_STRUCT *)target_value_ptr;
			OdbcInterval::SetMonth(interval, interval_struct);
			OdbcInterval::SetSignal(interval, interval_struct);
			return SQL_SUCCESS;
		}
		case SQL_C_INTERVAL_DAY: {
			interval_t interval;
			if (!OdbcInterval::GetInterval(val, interval, stmt)) {
				LogInvalidCast(val.type(), LogicalType::INTERVAL, stmt);
				return SQL_ERROR;
			}

			SQL_INTERVAL_STRUCT *interval_struct = (SQL_INTERVAL_STRUCT *)target_value_ptr;
			OdbcInterval::SetDay(interval, interval_struct);
			OdbcInterval::SetSignal(interval, interval_struct);
			return SQL_SUCCESS;
		}
		case SQL_C_INTERVAL_HOUR: {
			interval_t interval;
			if (!OdbcInterval::GetInterval(val, interval, stmt)) {
				LogInvalidCast(val.type(), LogicalType::INTERVAL, stmt);
				return SQL_ERROR;
			}

			SQL_INTERVAL_STRUCT *interval_struct = (SQL_INTERVAL_STRUCT *)target_value_ptr;
			OdbcInterval::SetHour(interval, interval_struct);
			OdbcInterval::SetSignal(interval, interval_struct);
			return SQL_SUCCESS;
		}
		case SQL_C_INTERVAL_MINUTE: {
			interval_t interval;
			if (!OdbcInterval::GetInterval(val, interval, stmt)) {
				LogInvalidCast(val.type(), LogicalType::INTERVAL, stmt);
				return SQL_ERROR;
			}

			SQL_INTERVAL_STRUCT *interval_struct = (SQL_INTERVAL_STRUCT *)target_value_ptr;
			OdbcInterval::SetMinute(interval, interval_struct);
			OdbcInterval::SetSignal(interval, interval_struct);
			return SQL_SUCCESS;
		}
		case SQL_C_INTERVAL_SECOND: {
			interval_t interval;
			if (!OdbcInterval::GetInterval(val, interval, stmt)) {
				LogInvalidCast(val.type(), LogicalType::INTERVAL, stmt);
				return SQL_ERROR;
			}

			SQL_INTERVAL_STRUCT *interval_struct = (SQL_INTERVAL_STRUCT *)target_value_ptr;
			OdbcInterval::SetSecond(interval, interval_struct);
			OdbcInterval::SetSignal(interval, interval_struct);
			return SQL_SUCCESS;
		}
		case SQL_C_INTERVAL_YEAR_TO_MONTH: {
			interval_t interval;
			if (!OdbcInterval::GetInterval(val, interval, stmt)) {
				LogInvalidCast(val.type(), LogicalType::INTERVAL, stmt);
				return SQL_ERROR;
			}

			SQL_INTERVAL_STRUCT *interval_struct = (SQL_INTERVAL_STRUCT *)target_value_ptr;
			OdbcInterval::SetYear(interval, interval_struct);
			// fraction of years stored as months
			interval_struct->intval.year_month.month = std::abs(interval.months) % duckdb::Interval::MONTHS_PER_YEAR;
			interval_struct->interval_type = SQLINTERVAL::SQL_IS_YEAR_TO_MONTH;
			OdbcInterval::SetSignal(interval, interval_struct);
			return SQL_SUCCESS;
		}
		case SQL_C_INTERVAL_DAY_TO_HOUR: {
			interval_t interval;
			if (!OdbcInterval::GetInterval(val, interval, stmt)) {
				LogInvalidCast(val.type(), LogicalType::INTERVAL, stmt);
				return SQL_ERROR;
			}

			SQL_INTERVAL_STRUCT *interval_struct = (SQL_INTERVAL_STRUCT *)target_value_ptr;
			OdbcInterval::SetDayToHour(interval, interval_struct);
			OdbcInterval::SetSignal(interval, interval_struct);
			return SQL_SUCCESS;
		}
		case SQL_C_INTERVAL_DAY_TO_MINUTE: {
			interval_t interval;
			if (!OdbcInterval::GetInterval(val, interval, stmt)) {
				LogInvalidCast(val.type(), LogicalType::INTERVAL, stmt);
				return SQL_ERROR;
			}

			SQL_INTERVAL_STRUCT *interval_struct = (SQL_INTERVAL_STRUCT *)target_value_ptr;
			OdbcInterval::SetDayToMinute(interval, interval_struct);
			OdbcInterval::SetSignal(interval, interval_struct);
			return SQL_SUCCESS;
		}
		case SQL_C_INTERVAL_DAY_TO_SECOND: {
			interval_t interval;
			if (!OdbcInterval::GetInterval(val, interval, stmt)) {
				LogInvalidCast(val.type(), LogicalType::INTERVAL, stmt);
				return SQL_ERROR;
			}

			SQL_INTERVAL_STRUCT *interval_struct = (SQL_INTERVAL_STRUCT *)target_value_ptr;
			OdbcInterval::SetDayToSecond(interval, interval_struct);
			OdbcInterval::SetSignal(interval, interval_struct);
			return SQL_SUCCESS;
		}
		case SQL_C_INTERVAL_HOUR_TO_MINUTE: {
			interval_t interval;
			if (!OdbcInterval::GetInterval(val, interval, stmt)) {
				LogInvalidCast(val.type(), LogicalType::INTERVAL, stmt);
				return SQL_ERROR;
			}

			SQL_INTERVAL_STRUCT *interval_struct = (SQL_INTERVAL_STRUCT *)target_value_ptr;
			OdbcInterval::SetHourToMinute(interval, interval_struct);
			OdbcInterval::SetSignal(interval, interval_struct);
			return SQL_SUCCESS;
		}
		case SQL_C_INTERVAL_HOUR_TO_SECOND: {
			interval_t interval;
			if (!OdbcInterval::GetInterval(val, interval, stmt)) {
				LogInvalidCast(val.type(), LogicalType::INTERVAL, stmt);
				return SQL_ERROR;
			}

			SQL_INTERVAL_STRUCT *interval_struct = (SQL_INTERVAL_STRUCT *)target_value_ptr;
			OdbcInterval::SetHourToSecond(interval, interval_struct);
			OdbcInterval::SetSignal(interval, interval_struct);
			return SQL_SUCCESS;
		}
		case SQL_C_INTERVAL_MINUTE_TO_SECOND: {
			interval_t interval;
			if (!OdbcInterval::GetInterval(val, interval, stmt)) {
				LogInvalidCast(val.type(), LogicalType::INTERVAL, stmt);
				return SQL_ERROR;
			}

			SQL_INTERVAL_STRUCT *interval_struct = (SQL_INTERVAL_STRUCT *)target_value_ptr;
			OdbcInterval::SetMinuteToSecond(interval, interval_struct);
			OdbcInterval::SetSignal(interval, interval_struct);
			return SQL_SUCCESS;
		}
		// TODO other types
		default:
			stmt->error_messages.emplace_back("Unsupported type.");
			return SQL_ERROR;

		} // end switch "(target_type)": SQL_C_TYPE_TIMESTAMP
	});
}