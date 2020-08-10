#include "duckdb/function/scalar/date_functions.hpp"

#include "duckdb/planner/expression/bound_function_expression.hpp"

#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types/numeric_helper.hpp"

#include "duckdb/common/vector_operations/unary_executor.hpp"

#include "duckdb/execution/expression_executor.hpp"

namespace duckdb {

enum class StrfTimeSpecifier : uint8_t {
	ABBREVIATED_WEEKDAY_NAME = 0,          // %a - Abbreviated weekday name. (Sun, Mon, ...)
	FULL_WEEKDAY_NAME = 1,                 // %A Full weekday name. (Sunday, Monday, ...)
	WEEKDAY_DECIMAL = 2,                   // %w - Weekday as a decimal number. (0, 1, ..., 6)
	DAY_OF_MONTH_PADDED = 3,               // %d - Day of the month as a zero-padded decimal. (01, 02, ..., 31)
	DAY_OF_MONTH = 4,                      // %-d - Day of the month as a decimal number. (1, 2, ..., 30)
	ABBREVIATED_MONTH_NAME = 5,            // %b - Abbreviated month name. (Jan, Feb, ..., Dec)
	FULL_MONTH_NAME = 6,                   // %B - Full month name. (January, February, ...)
	MONTH_DECIMAL_PADDED = 7,              // %m - Month as a zero-padded decimal number. (01, 02, ..., 12)
	MONTH_DECIMAL = 8,                     // %-m - Month as a decimal number. (1, 2, ..., 12)
	YEAR_WITHOUT_CENTURY_PADDED = 9,       // %y - Year without century as a zero-padded decimal number. (00, 01, ..., 99)
	YEAR_WITHOUT_CENTURY = 10,             // %-y - Year without century as a decimal number. (0, 1, ..., 99)
	YEAR_DECIMAL = 11,                     // %Y - Year with century as a decimal number. (2013, 2019 etc.)
	HOUR_24_PADDED = 12,                   // %H - Hour (24-hour clock) as a zero-padded decimal number. (00, 01, ..., 23)
	HOUR_24_DECIMAL = 13,                  // %-H - Hour (24-hour clock) as a decimal number. (0, 1, ..., 23)
	HOUR_12_PADDED = 14,                   // %I - Hour (12-hour clock) as a zero-padded decimal number. (01, 02, ..., 12)
	HOUR_12_DECIMAL = 15,                  // %-I - Hour (12-hour clock) as a decimal number. (1, 2, ... 12)
	AM_PM = 16,                            // %p - Locale’s AM or PM. (AM, PM)
	MINUTE_PADDED = 17,                    // %M - Minute as a zero-padded decimal number. (00, 01, ..., 59)
	MINUTE_DECIMAL = 18,                   // %-M - Minute as a decimal number. (0, 1, ..., 59)
	SECOND_PADDED = 19,                    // %S - Second as a zero-padded decimal number. (00, 01, ..., 59)
	SECOND_DECIMAL = 20,                   // %-S - Second as a decimal number. (0, 1, ..., 59)
	MICROSECOND_PADDED = 21,               // %f - Microsecond as a decimal number, zero-padded on the left. (000000 - 999999)
	UTC_OFFSET = 22,                       // %z - UTC offset in the form +HHMM or -HHMM. ( )
	TZ_NAME = 23,                          // %Z - Time zone name. ( )
	DAY_OF_YEAR_PADDED = 24,               // %j - Day of the year as a zero-padded decimal number. (001, 002, ..., 366)
	DAY_OF_YEAR_DECIMAL = 25,              // %-j - Day of the year as a decimal number. (1, 2, ..., 366)
	WEEK_NUMBER_PADDED_SUN_FIRST = 26,     // %U - Week number of the year (Sunday as the first day of the week). All days in a new year preceding the first Sunday are considered to be in week 0. (00, 01, ..., 53)
	WEEK_NUMBER_PADDED_MON_FIRST = 27,     // %W - Week number of the year (Monday as the first day of the week). All days in a new year preceding the first Monday are considered to be in week 0. (00, 01, ..., 53)
	LOCALE_APPROPRIATE_DATE_AND_TIME = 28, // %c - Locale’s appropriate date and time representation. (Mon Sep 30 07:06:05 2013)
	LOCALE_APPROPRIATE_DATE = 29,          // %x - Locale’s appropriate date representation. (09/30/13)
	LOCALE_APPROPRIATE_TIME = 30           // %X - Locale’s appropriate time representation. (07:06:05)
};

idx_t StrfTimepecifierSize(StrfTimeSpecifier specifier) {
	switch(specifier) {
	case StrfTimeSpecifier::ABBREVIATED_WEEKDAY_NAME:
	case StrfTimeSpecifier::ABBREVIATED_MONTH_NAME:
		return 3;
	case StrfTimeSpecifier::WEEKDAY_DECIMAL:
		return 1;
	case StrfTimeSpecifier::DAY_OF_MONTH_PADDED:
	case StrfTimeSpecifier::MONTH_DECIMAL_PADDED:
	case StrfTimeSpecifier::YEAR_WITHOUT_CENTURY_PADDED:
	case StrfTimeSpecifier::HOUR_24_PADDED:
	case StrfTimeSpecifier::HOUR_12_PADDED:
	case StrfTimeSpecifier::MINUTE_PADDED:
	case StrfTimeSpecifier::SECOND_PADDED:
	case StrfTimeSpecifier::AM_PM:
	case StrfTimeSpecifier::WEEK_NUMBER_PADDED_SUN_FIRST:
	case StrfTimeSpecifier::WEEK_NUMBER_PADDED_MON_FIRST:
		return 2;
	case StrfTimeSpecifier::MICROSECOND_PADDED:
		return 6;
	case StrfTimeSpecifier::DAY_OF_YEAR_PADDED:
		return 3;
	default:
		return 0;
	}
}

struct StrfTimeFormat {
	//! The format specifiers
	vector<StrfTimeSpecifier> specifiers;
	//! The literals that appear in between the format specifiers
	//! The following must hold: literals.size() = specifiers.size() + 1
	//! Format is literals[0], specifiers[0], literals[1], ..., specifiers[n - 1], literals[n]
	vector<string> literals;
	//! The constant size that appears in the format string
	idx_t constant_size;
	//! The variable-length specifiers. To determine total string size, these need to be checked.
	vector<StrfTimeSpecifier> var_length_specifiers;
	//! Whether or not the current specifier is a special "date" specifier (i.e. one that requires a date_t object to generate)
	vector<bool> is_date_specifier;


	void AddLiteral(string literal) {
		constant_size += literal.size();
		literals.push_back(move(literal));
	}

	void AddFormatSpecifier(string preceding_literal, StrfTimeSpecifier specifier) {
		AddLiteral(move(preceding_literal));
		specifiers.push_back(specifier);
		is_date_specifier.push_back(IsDateSpecifier(specifier));
		idx_t specifier_size = StrfTimepecifierSize(specifier);
		if (specifier_size == 0) {
			// variable length specifier
			var_length_specifiers.push_back(specifier);
		} else {
			// constant size specifier
			constant_size += specifier_size;
		}
	}

	static idx_t GetSpecifierLength(StrfTimeSpecifier specifier, date_t &date) {
		switch(specifier) {
		case StrfTimeSpecifier::FULL_WEEKDAY_NAME:
			return Date::DayNames[Date::ExtractISODayOfTheWeek(date) % 7].GetSize();
		case StrfTimeSpecifier::FULL_MONTH_NAME:
			return Date::MonthNames[Date::ExtractMonth(date) - 1].GetSize();
		case StrfTimeSpecifier::YEAR_DECIMAL: {
			auto year = Date::ExtractYear(date);
			return NumericHelper::SignedLength<int32_t, uint32_t>(year);
		}
		case StrfTimeSpecifier::MONTH_DECIMAL: {
			idx_t len = 1;
			auto month = Date::ExtractMonth(date);
			len += month >= 10;
			return len;
		}
		case StrfTimeSpecifier::UTC_OFFSET:
		case StrfTimeSpecifier::TZ_NAME:
			// empty for date
			return 0;
		case StrfTimeSpecifier::HOUR_24_DECIMAL: // 0
			return 1;
		case StrfTimeSpecifier::HOUR_12_DECIMAL: // 12
			return 2;
		case StrfTimeSpecifier::MINUTE_DECIMAL: // 0
			return 1;
		case StrfTimeSpecifier::SECOND_DECIMAL: // 0
			return 1;
		case StrfTimeSpecifier::DAY_OF_MONTH:
			return NumericHelper::UnsignedLength<uint32_t>(Date::ExtractDay(date));
		case StrfTimeSpecifier::DAY_OF_YEAR_DECIMAL:
			return NumericHelper::UnsignedLength<uint32_t>(Date::ExtractDayOfTheYear(date));
		case StrfTimeSpecifier::YEAR_WITHOUT_CENTURY:
			return NumericHelper::UnsignedLength<uint32_t>(Date::ExtractYear(date) % 100);
		default:
			throw NotImplementedException("Unimplemented specifier for GetSpecifierLength");
		}
	}

	//! Returns the total length of the date formatted by this format specifier
	idx_t GetLength(date_t date) {
		idx_t size = constant_size;
		if (var_length_specifiers.size() > 0) {
			for(auto &specifier : var_length_specifiers) {
				size += GetSpecifierLength(specifier, date);
			}
		}
		return size;
	}

	char* WriteString(char *target, string_t &str) {
		idx_t size = str.GetSize();
		memcpy(target, str.GetData(), str.GetSize());
		return target + size;
	}

	// write a value in the range of 0..99 unpadded (e.g. "1", "2", ... "98", "99")
	char *Write2(char *target, uint8_t value) {
		if (value >= 10) {
			return WritePadded2(target, value);
		} else {
			*target = '0' + value;
			return target + 1;
		}
	}

	// write a value in the range of 0..99 padded to 2 digits
	char* WritePadded2(char *target, int32_t value) {
		auto index = static_cast<unsigned>(value * 2);
		*target++ = duckdb_fmt::internal::data::digits[index];
		*target++ = duckdb_fmt::internal::data::digits[index + 1];
		return target;
	}

	// write a value in the range of 0..999 padded
	char *WritePadded3(char *target, uint32_t value) {
		if (value >= 100) {
			WritePadded2(target + 1, value % 100);
			*target = '0' + value / 100;
			return target + 3;
		} else {
			*target = '0';
			target++;
			return WritePadded2(target, value);
		}
	}

	// write a value in the range of 0..999999 padded to 6 digits
	char* WritePadded(char *target, int32_t value, int32_t padding) {
		assert(padding % 2 == 0);
		for(int i = 0; i < padding / 2; i++) {
			int decimals = value % 100;
			WritePadded2(target + padding - 2 * (i + 1), decimals);
			value /= 100;
		}
		return target + padding;
	}

	bool IsDateSpecifier(StrfTimeSpecifier specifier) {
		switch(specifier) {
		case StrfTimeSpecifier::ABBREVIATED_WEEKDAY_NAME:
		case StrfTimeSpecifier::FULL_WEEKDAY_NAME:
		case StrfTimeSpecifier::WEEKDAY_DECIMAL:
		case StrfTimeSpecifier::DAY_OF_YEAR_PADDED:
		case StrfTimeSpecifier::WEEK_NUMBER_PADDED_MON_FIRST:
		case StrfTimeSpecifier::WEEK_NUMBER_PADDED_SUN_FIRST:
		case StrfTimeSpecifier::DAY_OF_YEAR_DECIMAL:
			return true;
		default:
			return false;
		}
	}

	char* WriteDateSpecifier(StrfTimeSpecifier specifier, date_t date, char *target) {
		switch(specifier) {
		case StrfTimeSpecifier::ABBREVIATED_WEEKDAY_NAME: {
			date_t dow = Date::ExtractISODayOfTheWeek(date);
			target = WriteString(target, Date::DayNamesAbbreviated[dow % 7]);
			break;
		}
		case StrfTimeSpecifier::FULL_WEEKDAY_NAME: {
			date_t dow = Date::ExtractISODayOfTheWeek(date);
			target = WriteString(target, Date::DayNames[dow % 7]);
			break;
		}
		case StrfTimeSpecifier::WEEKDAY_DECIMAL: {
			date_t dow = Date::ExtractISODayOfTheWeek(date);
			*target = '0' + (dow % 7);
			target++;
			break;
		}
		case StrfTimeSpecifier::DAY_OF_YEAR_PADDED: {
			int32_t doy = Date::ExtractDayOfTheYear(date);
			target = WritePadded3(target, doy);
			break;
		}
		case StrfTimeSpecifier::WEEK_NUMBER_PADDED_MON_FIRST:
			target = WritePadded2(target, Date::ExtractWeekNumberRegular(date, true));
			break;
		case StrfTimeSpecifier::WEEK_NUMBER_PADDED_SUN_FIRST:
			target = WritePadded2(target, Date::ExtractWeekNumberRegular(date, false));
			break;
		case StrfTimeSpecifier::DAY_OF_YEAR_DECIMAL: {
			uint32_t doy = Date::ExtractDayOfTheYear(date);
			target += NumericHelper::UnsignedLength<uint32_t>(doy);
			NumericHelper::FormatUnsigned(doy, target);
			break;
		}
		default:
			throw NotImplementedException("Unimplemented date specifier for strftime");
		}
		return target;
	}

	char* WriteStandardSpecifier(StrfTimeSpecifier specifier, int32_t data[], char *target) {
		// data contains [0] year, [1] month, [2] day, [3] hour, [4] minute, [5] second, [6] msec
		switch(specifier) {
		case StrfTimeSpecifier::DAY_OF_MONTH_PADDED:
			target = WritePadded2(target, data[2]);
			break;
		case StrfTimeSpecifier::ABBREVIATED_MONTH_NAME: {
			auto &month_name = Date::MonthNamesAbbreviated[data[1] - 1];
			return WriteString(target, month_name);
		}
		case StrfTimeSpecifier::FULL_MONTH_NAME: {
			auto &month_name = Date::MonthNames[data[1] - 1];
			return WriteString(target, month_name);
		}
		case StrfTimeSpecifier::MONTH_DECIMAL_PADDED:
			target = WritePadded2(target, data[1]);
			break;
		case StrfTimeSpecifier::YEAR_WITHOUT_CENTURY_PADDED:
			target = WritePadded2(target, data[0] % 100);
			break;
		case StrfTimeSpecifier::YEAR_DECIMAL:
			if (data[0] >= 0 && data[0] <= 9999) {
				target = WritePadded(target, data[0], 4);
			} else {
				int32_t year = data[0];
				if (data[0] < 0) {
					*target = '-';
					year = -year;
					target++;
				}
				auto len = NumericHelper::UnsignedLength<uint32_t>(year);
				NumericHelper::FormatUnsigned(year, target + len);
				target += len;
			}
			break;
		case StrfTimeSpecifier::HOUR_24_PADDED: {
			target = WritePadded2(target, data[4]);
			break;
		}
		case StrfTimeSpecifier::HOUR_12_PADDED: {
			int hour = data[3] % 12;
			if (hour == 0) {
				hour = 12;
			}
			target = WritePadded2(target, hour);
			break;
		}
		case StrfTimeSpecifier::AM_PM:
			*target++ = data[3] > 12 ? 'P' : 'A';
			*target++ = 'M';
			break;
		case StrfTimeSpecifier::MINUTE_PADDED: {
			target = WritePadded2(target, data[4]);
			break;
		}
		case StrfTimeSpecifier::SECOND_PADDED:
			target = WritePadded2(target, data[5]);
			break;
		case StrfTimeSpecifier::MICROSECOND_PADDED:
			target = WritePadded(target, data[6] * 1000, 6);
			break;
		case StrfTimeSpecifier::UTC_OFFSET:
		case StrfTimeSpecifier::TZ_NAME:
			// always empty for now, FIXME when we have timestamp with tz
			break;
		case StrfTimeSpecifier::DAY_OF_MONTH: {
			target = Write2(target, data[2] % 100);
			break;
		}
		case StrfTimeSpecifier::MONTH_DECIMAL: {
			target = Write2(target, data[1]);
			break;
		}
		case StrfTimeSpecifier::YEAR_WITHOUT_CENTURY: {
			target = Write2(target, data[0] % 100);
			break;
		}
		case StrfTimeSpecifier::HOUR_24_DECIMAL: {
			target = Write2(target, data[3]);
			break;
		}
		case StrfTimeSpecifier::HOUR_12_DECIMAL: {
			int hour = data[3] % 12;
			if (hour == 0) {
				hour = 12;
			}
			target = Write2(target, hour);
			break;
		}
		case StrfTimeSpecifier::MINUTE_DECIMAL: {
			target = Write2(target, data[4]);
			break;
		}
		case StrfTimeSpecifier::SECOND_DECIMAL: {
			target = Write2(target, data[5]);
			break;
		}
		default:
			throw NotImplementedException("Unimplemented specifier for WriteStandardSpecifier in strftime");
		}
		return target;
	}

	void FormatString(date_t date, int32_t data[7], char *target) {
		idx_t i;
		for(i = 0; i < specifiers.size(); i++) {
			// first copy the current literal
			memcpy(target, literals[i].c_str(), literals[i].size());
			target += literals[i].size();
			// now copy the specifier
			if (is_date_specifier[i]) {
				target = WriteDateSpecifier(specifiers[i], date, target);
			} else {
				target = WriteStandardSpecifier(specifiers[i], data, target);
			}
		}
		// copy the final literal into the target
		memcpy(target, literals[i].c_str(), literals[i].size());

	}

	void FormatString(date_t date, char *target) {
		int32_t data[7]; // year, month, day, hour, min, sec, msec
		Date::Convert(date, data[0], data[1], data[2]);
		data[3] = data[4] = data[5] = data[6] = 0;

		FormatString(date, data, target);
	}

	void FormatString(timestamp_t timestamp, char *target) {
		int32_t data[7]; // year, month, day, hour, min, sec, msec
		date_t date;
		dtime_t time;
		Timestamp::Convert(timestamp, date, time);
		Date::Convert(date, data[0], data[1], data[2]);
		Time::Convert(time, data[3], data[4], data[5], data[6]);

		FormatString(date, data, target);
	}
};

string ParseFormatSpecifier(string format_string, StrfTimeFormat &format) {
	format.constant_size = 0;
	idx_t pos = 0;
	string current_literal;
	for(idx_t i = 0; i < format_string.size(); i++) {
		if (format_string[i] == '%') {
			if (i + 1 == format_string.size()) {
				return "Trailing format character %";
			}
			if (i > pos) {
				// push the previous string to the current literal
				current_literal += format_string.substr(pos, i - pos);
			}
			char format_char = format_string[++i];
			if (format_char == '%') {
				// special case: %%
				// set the pos for the next literal and continue
				pos = i;
				continue;
			}
			StrfTimeSpecifier specifier;
			if (format_char == '-' && i + 1 < format_string.size()) {
				format_char = format_string[++i];
				switch(format_char) {
				case 'd':
					specifier = StrfTimeSpecifier::DAY_OF_MONTH;
					break;
				case 'm':
					specifier = StrfTimeSpecifier::MONTH_DECIMAL;
					break;
				case 'y':
					specifier = StrfTimeSpecifier::YEAR_WITHOUT_CENTURY;
					break;
				case 'H':
					specifier = StrfTimeSpecifier::HOUR_24_DECIMAL;
					break;
				case 'I':
					specifier = StrfTimeSpecifier::HOUR_12_DECIMAL;
					break;
				case 'M':
					specifier = StrfTimeSpecifier::MINUTE_DECIMAL;
					break;
				case 'S':
					specifier = StrfTimeSpecifier::SECOND_DECIMAL;
					break;
				case 'j':
					specifier = StrfTimeSpecifier::DAY_OF_YEAR_DECIMAL;
					break;
				default:
					return "Unrecognized format for strftime/strptime: %-" + string(format_char, 1);
				}
			} else {
				switch(format_char) {
				case 'a':
					specifier = StrfTimeSpecifier::ABBREVIATED_WEEKDAY_NAME;
					break;
				case 'A':
					specifier = StrfTimeSpecifier::FULL_WEEKDAY_NAME;
					break;
				case 'w':
					specifier = StrfTimeSpecifier::WEEKDAY_DECIMAL;
					break;
				case 'd':
					specifier = StrfTimeSpecifier::DAY_OF_MONTH_PADDED;
					break;
				case 'h':
				case 'b':
					specifier = StrfTimeSpecifier::ABBREVIATED_MONTH_NAME;
					break;
				case 'B':
					specifier = StrfTimeSpecifier::FULL_MONTH_NAME;
					break;
				case 'm':
					specifier = StrfTimeSpecifier::MONTH_DECIMAL_PADDED;
					break;
				case 'y':
					specifier = StrfTimeSpecifier::YEAR_WITHOUT_CENTURY_PADDED;
					break;
				case 'Y':
					specifier = StrfTimeSpecifier::YEAR_DECIMAL;
					break;
				case 'H':
					specifier = StrfTimeSpecifier::HOUR_24_PADDED;
					break;
				case 'I':
					specifier = StrfTimeSpecifier::HOUR_12_PADDED;
					break;
				case 'p':
					specifier = StrfTimeSpecifier::AM_PM;
					break;
				case 'M':
					specifier = StrfTimeSpecifier::MINUTE_PADDED;
					break;
				case 'S':
					specifier = StrfTimeSpecifier::SECOND_PADDED;
					break;
				case 'f':
					specifier = StrfTimeSpecifier::MICROSECOND_PADDED;
					break;
				case 'z':
					specifier = StrfTimeSpecifier::UTC_OFFSET;
					break;
				case 'Z':
					specifier = StrfTimeSpecifier::TZ_NAME;
					break;
				case 'j':
					specifier = StrfTimeSpecifier::DAY_OF_YEAR_PADDED;
					break;
				case 'U':
					specifier = StrfTimeSpecifier::WEEK_NUMBER_PADDED_SUN_FIRST;
					break;
				case 'W':
					specifier = StrfTimeSpecifier::WEEK_NUMBER_PADDED_MON_FIRST;
					break;
				case 'c':
				case 'x':
				case 'X': {
					string subformat;
					if (format_char == 'c') {
						// %c: Locale’s appropriate date and time representation.
						// we push the ISO timestamp representation here
						subformat = "%Y-%m-%d %H:%M:%S";
					} else if (format_char == 'x') {
						// %x - Locale’s appropriate date representation.
						// we push the ISO date format here
						subformat = "%Y-%m-%d";
					} else if (format_char == 'X') {
						// %X - Locale’s appropriate time representation.
						// we push the ISO time format here
						subformat = "%H:%M:%S";
					}
					// parse the subformat in a separate format specifier
					StrfTimeFormat locale_format;
					string error = ParseFormatSpecifier(subformat, locale_format);
					assert(error.empty());
					// add the previous literal to the first literal of the subformat
					locale_format.literals[0] = move(current_literal) + locale_format.literals[0];
					// now push the subformat into the current format specifier
					for(idx_t i = 0; i < locale_format.specifiers.size(); i++) {
						format.AddFormatSpecifier(move(locale_format.literals[i]), locale_format.specifiers[i]);
					}
					pos = i + 1;
					continue;
				}
				default:
					return "Unrecognized format for strftime/strptime: %" + string(format_char, 1);
				}
			}
			format.AddFormatSpecifier(move(current_literal), specifier);
			pos = i + 1;
		}
	}
	// add the final literal
	if (pos < format_string.size()) {
		current_literal += format_string.substr(pos, format_string.size() - pos);
	}
	format.AddLiteral(move(current_literal));
	return string();
}

struct StrfTimeBindData : public FunctionData {
	StrfTimeBindData(StrfTimeFormat format) : format(move(format)) {}

	StrfTimeFormat format;

	unique_ptr<FunctionData> Copy() override {
		return make_unique<StrfTimeBindData>(format);
	}
};

static unique_ptr<FunctionData> strftime_bind_function(BoundFunctionExpression &expr, ClientContext &context) {
	if (!expr.children[0]->IsScalar()) {
		throw InvalidInputException("strftime format must be a constant");
	}
	Value options_str = ExpressionExecutor::EvaluateScalar(*expr.children[0]);
	StrfTimeFormat format;
	if (!options_str.is_null && options_str.type == TypeId::VARCHAR) {
		string error = ParseFormatSpecifier(options_str.str_value, format);
		if (!error.empty()) {
			throw InvalidInputException("Failed to parse format specifier %s: %s", options_str.str_value.c_str(), error.c_str());
		}
	}
	return make_unique<StrfTimeBindData>(format);
}

template<class T>
static void strftime_function(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = (BoundFunctionExpression &)state.expr;
	auto &info = (StrfTimeBindData &)*func_expr.bind_info;

	if (ConstantVector::IsNull(args.data[0])) {
		result.vector_type = VectorType::CONSTANT_VECTOR;
		ConstantVector::SetNull(result, true);
		return;
	}

	UnaryExecutor::Execute<T, string_t, true>(args.data[1], result, args.size(), [&](T date) {
		idx_t len = info.format.GetLength(date);
		string_t target = StringVector::EmptyString(result, len);
		info.format.FormatString(date, target.GetData());
		target.Finalize();
		return target;
	});
}


void StrfTimeFun::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunctionSet strftime("strftime");

	strftime.AddFunction(ScalarFunction({SQLType::VARCHAR, SQLType::DATE}, SQLType::VARCHAR,
	                               strftime_function<date_t>, false, strftime_bind_function));

	strftime.AddFunction(ScalarFunction({SQLType::VARCHAR, SQLType::TIMESTAMP}, SQLType::VARCHAR,
	                               strftime_function<timestamp_t>, false, strftime_bind_function));

	set.AddFunction(strftime);
}

}