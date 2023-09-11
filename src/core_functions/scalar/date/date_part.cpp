#include "duckdb/core_functions/scalar/date_functions.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/enums/date_part_specifier.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

namespace duckdb {

DatePartSpecifier GetDateTypePartSpecifier(const string &specifier, LogicalType &type) {
	const auto part = GetDatePartSpecifier(specifier);
	switch (type.id()) {
	case LogicalType::TIMESTAMP:
	case LogicalType::TIMESTAMP_TZ:
		return part;
	case LogicalType::DATE:
		switch (part) {
		case DatePartSpecifier::YEAR:
		case DatePartSpecifier::MONTH:
		case DatePartSpecifier::DAY:
		case DatePartSpecifier::DECADE:
		case DatePartSpecifier::CENTURY:
		case DatePartSpecifier::MILLENNIUM:
		case DatePartSpecifier::DOW:
		case DatePartSpecifier::ISODOW:
		case DatePartSpecifier::ISOYEAR:
		case DatePartSpecifier::WEEK:
		case DatePartSpecifier::QUARTER:
		case DatePartSpecifier::DOY:
		case DatePartSpecifier::YEARWEEK:
		case DatePartSpecifier::ERA:
		case DatePartSpecifier::JULIAN_DAY:
			return part;
		default:
			break;
		}
		break;
	case LogicalType::TIME:
		switch (part) {
		case DatePartSpecifier::MICROSECONDS:
		case DatePartSpecifier::MILLISECONDS:
		case DatePartSpecifier::SECOND:
		case DatePartSpecifier::MINUTE:
		case DatePartSpecifier::HOUR:
		case DatePartSpecifier::EPOCH:
		case DatePartSpecifier::TIMEZONE:
		case DatePartSpecifier::TIMEZONE_HOUR:
		case DatePartSpecifier::TIMEZONE_MINUTE:
			return part;
		default:
			break;
		}
		break;
	case LogicalType::INTERVAL:
		switch (part) {
		case DatePartSpecifier::YEAR:
		case DatePartSpecifier::MONTH:
		case DatePartSpecifier::DAY:
		case DatePartSpecifier::DECADE:
		case DatePartSpecifier::CENTURY:
		case DatePartSpecifier::QUARTER:
		case DatePartSpecifier::MILLENNIUM:
		case DatePartSpecifier::MICROSECONDS:
		case DatePartSpecifier::MILLISECONDS:
		case DatePartSpecifier::SECOND:
		case DatePartSpecifier::MINUTE:
		case DatePartSpecifier::HOUR:
		case DatePartSpecifier::EPOCH:
			return part;
		default:
			break;
		}
		break;
	default:
		break;
	}

	throw NotImplementedException("\"%s\" units \"%s\" not recognized", EnumUtil::ToString(type.id()), specifier);
}

template <int64_t MIN, int64_t MAX>
static unique_ptr<BaseStatistics> PropagateSimpleDatePartStatistics(vector<BaseStatistics> &child_stats) {
	// we can always propagate simple date part statistics
	// since the min and max can never exceed these bounds
	auto result = NumericStats::CreateEmpty(LogicalType::BIGINT);
	result.CopyValidity(child_stats[0]);
	NumericStats::SetMin(result, Value::BIGINT(MIN));
	NumericStats::SetMax(result, Value::BIGINT(MAX));
	return result.ToUnique();
}

struct DatePart {
	template <class T, class OP, class TR = int64_t>
	static unique_ptr<BaseStatistics> PropagateDatePartStatistics(vector<BaseStatistics> &child_stats,
	                                                              const LogicalType &stats_type = LogicalType::BIGINT) {
		// we can only propagate complex date part stats if the child has stats
		auto &nstats = child_stats[0];
		if (!NumericStats::HasMinMax(nstats)) {
			return nullptr;
		}
		// run the operator on both the min and the max, this gives us the [min, max] bound
		auto min = NumericStats::GetMin<T>(nstats);
		auto max = NumericStats::GetMax<T>(nstats);
		if (min > max) {
			return nullptr;
		}
		// Infinities prevent us from computing generic ranges
		if (!Value::IsFinite(min) || !Value::IsFinite(max)) {
			return nullptr;
		}
		TR min_part = OP::template Operation<T, TR>(min);
		TR max_part = OP::template Operation<T, TR>(max);
		auto result = NumericStats::CreateEmpty(stats_type);
		NumericStats::SetMin(result, Value(min_part));
		NumericStats::SetMax(result, Value(max_part));
		result.CopyValidity(child_stats[0]);
		return result.ToUnique();
	}

	template <typename OP>
	struct PartOperator {
		template <class TA, class TR>
		static inline TR Operation(TA input, ValidityMask &mask, idx_t idx, void *dataptr) {
			if (Value::IsFinite(input)) {
				return OP::template Operation<TA, TR>(input);
			} else {
				mask.SetInvalid(idx);
				return TR();
			}
		}
	};

	template <class TA, class TR, class OP>
	static void UnaryFunction(DataChunk &input, ExpressionState &state, Vector &result) {
		D_ASSERT(input.ColumnCount() >= 1);
		using IOP = PartOperator<OP>;
		UnaryExecutor::GenericExecute<TA, TR, IOP>(input.data[0], result, input.size(), nullptr, true);
	}

	struct YearOperator {
		template <class TA, class TR>
		static inline TR Operation(TA input) {
			return Date::ExtractYear(input);
		}

		template <class T>
		static unique_ptr<BaseStatistics> PropagateStatistics(ClientContext &context, FunctionStatisticsInput &input) {
			return PropagateDatePartStatistics<T, YearOperator>(input.child_stats);
		}
	};

	struct MonthOperator {
		template <class TA, class TR>
		static inline TR Operation(TA input) {
			return Date::ExtractMonth(input);
		}

		template <class T>
		static unique_ptr<BaseStatistics> PropagateStatistics(ClientContext &context, FunctionStatisticsInput &input) {
			// min/max of month operator is [1, 12]
			return PropagateSimpleDatePartStatistics<1, 12>(input.child_stats);
		}
	};

	struct DayOperator {
		template <class TA, class TR>
		static inline TR Operation(TA input) {
			return Date::ExtractDay(input);
		}

		template <class T>
		static unique_ptr<BaseStatistics> PropagateStatistics(ClientContext &context, FunctionStatisticsInput &input) {
			// min/max of day operator is [1, 31]
			return PropagateSimpleDatePartStatistics<1, 31>(input.child_stats);
		}
	};

	struct DecadeOperator {
		// From the PG docs: "The year field divided by 10"
		template <typename TR>
		static inline TR DecadeFromYear(TR yyyy) {
			return yyyy / 10;
		}

		template <class TA, class TR>
		static inline TR Operation(TA input) {
			return DecadeFromYear(YearOperator::Operation<TA, TR>(input));
		}

		template <class T>
		static unique_ptr<BaseStatistics> PropagateStatistics(ClientContext &context, FunctionStatisticsInput &input) {
			return PropagateDatePartStatistics<T, DecadeOperator>(input.child_stats);
		}
	};

	struct CenturyOperator {
		// From the PG docs:
		// "The first century starts at 0001-01-01 00:00:00 AD, although they did not know it at the time.
		// This definition applies to all Gregorian calendar countries.
		// There is no century number 0, you go from -1 century to 1 century.
		// If you disagree with this, please write your complaint to: Pope, Cathedral Saint-Peter of Roma, Vatican."
		// (To be fair, His Holiness had nothing to do with this -
		// it was the lack of zero in the counting systems of the time...)
		template <typename TR>
		static inline TR CenturyFromYear(TR yyyy) {
			if (yyyy > 0) {
				return ((yyyy - 1) / 100) + 1;
			} else {
				return (yyyy / 100) - 1;
			}
		}

		template <class TA, class TR>
		static inline TR Operation(TA input) {
			return CenturyFromYear(YearOperator::Operation<TA, TR>(input));
		}

		template <class T>
		static unique_ptr<BaseStatistics> PropagateStatistics(ClientContext &context, FunctionStatisticsInput &input) {
			return PropagateDatePartStatistics<T, CenturyOperator>(input.child_stats);
		}
	};

	struct MillenniumOperator {
		// See the century comment
		template <typename TR>
		static inline TR MillenniumFromYear(TR yyyy) {
			if (yyyy > 0) {
				return ((yyyy - 1) / 1000) + 1;
			} else {
				return (yyyy / 1000) - 1;
			}
		}

		template <class TA, class TR>
		static inline TR Operation(TA input) {
			return MillenniumFromYear<TR>(YearOperator::Operation<TA, TR>(input));
		}

		template <class T>
		static unique_ptr<BaseStatistics> PropagateStatistics(ClientContext &context, FunctionStatisticsInput &input) {
			return PropagateDatePartStatistics<T, MillenniumOperator>(input.child_stats);
		}
	};

	struct QuarterOperator {
		template <class TR>
		static inline TR QuarterFromMonth(TR mm) {
			return (mm - 1) / Interval::MONTHS_PER_QUARTER + 1;
		}

		template <class TA, class TR>
		static inline TR Operation(TA input) {
			return QuarterFromMonth(Date::ExtractMonth(input));
		}

		template <class T>
		static unique_ptr<BaseStatistics> PropagateStatistics(ClientContext &context, FunctionStatisticsInput &input) {
			// min/max of quarter operator is [1, 4]
			return PropagateSimpleDatePartStatistics<1, 4>(input.child_stats);
		}
	};

	struct DayOfWeekOperator {
		template <class TR>
		static inline TR DayOfWeekFromISO(TR isodow) {
			// day of the week (Sunday = 0, Saturday = 6)
			// turn sunday into 0 by doing mod 7
			return isodow % 7;
		}

		template <class TA, class TR>
		static inline TR Operation(TA input) {
			return DayOfWeekFromISO(Date::ExtractISODayOfTheWeek(input));
		}

		template <class T>
		static unique_ptr<BaseStatistics> PropagateStatistics(ClientContext &context, FunctionStatisticsInput &input) {
			return PropagateSimpleDatePartStatistics<0, 6>(input.child_stats);
		}
	};

	struct ISODayOfWeekOperator {
		template <class TA, class TR>
		static inline TR Operation(TA input) {
			// isodow (Monday = 1, Sunday = 7)
			return Date::ExtractISODayOfTheWeek(input);
		}

		template <class T>
		static unique_ptr<BaseStatistics> PropagateStatistics(ClientContext &context, FunctionStatisticsInput &input) {
			return PropagateSimpleDatePartStatistics<1, 7>(input.child_stats);
		}
	};

	struct DayOfYearOperator {
		template <class TA, class TR>
		static inline TR Operation(TA input) {
			return Date::ExtractDayOfTheYear(input);
		}

		template <class T>
		static unique_ptr<BaseStatistics> PropagateStatistics(ClientContext &context, FunctionStatisticsInput &input) {
			return PropagateSimpleDatePartStatistics<1, 366>(input.child_stats);
		}
	};

	struct WeekOperator {
		template <class TA, class TR>
		static inline TR Operation(TA input) {
			return Date::ExtractISOWeekNumber(input);
		}

		template <class T>
		static unique_ptr<BaseStatistics> PropagateStatistics(ClientContext &context, FunctionStatisticsInput &input) {
			return PropagateSimpleDatePartStatistics<1, 54>(input.child_stats);
		}
	};

	struct ISOYearOperator {
		template <class TA, class TR>
		static inline TR Operation(TA input) {
			return Date::ExtractISOYearNumber(input);
		}

		template <class T>
		static unique_ptr<BaseStatistics> PropagateStatistics(ClientContext &context, FunctionStatisticsInput &input) {
			return PropagateDatePartStatistics<T, ISOYearOperator>(input.child_stats);
		}
	};

	struct YearWeekOperator {
		template <class TR>
		static inline TR YearWeekFromParts(TR yyyy, TR ww) {
			return yyyy * 100 + ((yyyy > 0) ? ww : -ww);
		}

		template <class TA, class TR>
		static inline TR Operation(TA input) {
			int32_t yyyy, ww;
			Date::ExtractISOYearWeek(input, yyyy, ww);
			return YearWeekFromParts(yyyy, ww);
		}

		template <class T>
		static unique_ptr<BaseStatistics> PropagateStatistics(ClientContext &context, FunctionStatisticsInput &input) {
			return PropagateDatePartStatistics<T, YearWeekOperator>(input.child_stats);
		}
	};

	struct EpochNanosecondsOperator {
		template <class TA, class TR>
		static inline TR Operation(TA input) {
			return input.micros * Interval::NANOS_PER_MICRO;
		}

		template <class T>
		static unique_ptr<BaseStatistics> PropagateStatistics(ClientContext &context, FunctionStatisticsInput &input) {
			return PropagateDatePartStatistics<T, EpochNanosecondsOperator>(input.child_stats);
		}
	};

	struct EpochMicrosecondsOperator {
		template <class TA, class TR>
		static inline TR Operation(TA input) {
			return input.micros;
		}

		template <class T>
		static unique_ptr<BaseStatistics> PropagateStatistics(ClientContext &context, FunctionStatisticsInput &input) {
			return PropagateDatePartStatistics<T, EpochMicrosecondsOperator>(input.child_stats);
		}
	};

	struct EpochMillisOperator {
		template <class TA, class TR>
		static inline TR Operation(TA input) {
			return input.micros / Interval::MICROS_PER_MSEC;
		}

		template <class T>
		static unique_ptr<BaseStatistics> PropagateStatistics(ClientContext &context, FunctionStatisticsInput &input) {
			return PropagateDatePartStatistics<T, EpochMillisOperator>(input.child_stats);
		}

		static void Inverse(DataChunk &input, ExpressionState &state, Vector &result) {
			D_ASSERT(input.ColumnCount() == 1);

			UnaryExecutor::Execute<int64_t, timestamp_t>(input.data[0], result, input.size(),
			                                             [&](int64_t input) { return Timestamp::FromEpochMs(input); });
		}
	};

	struct MicrosecondsOperator {
		template <class TA, class TR>
		static inline TR Operation(TA input) {
			return 0;
		}

		template <class T>
		static unique_ptr<BaseStatistics> PropagateStatistics(ClientContext &context, FunctionStatisticsInput &input) {
			return PropagateSimpleDatePartStatistics<0, 60000000>(input.child_stats);
		}
	};

	struct MillisecondsOperator {
		template <class TA, class TR>
		static inline TR Operation(TA input) {
			return 0;
		}

		template <class T>
		static unique_ptr<BaseStatistics> PropagateStatistics(ClientContext &context, FunctionStatisticsInput &input) {
			return PropagateSimpleDatePartStatistics<0, 60000>(input.child_stats);
		}
	};

	struct SecondsOperator {
		template <class TA, class TR>
		static inline TR Operation(TA input) {
			return 0;
		}

		template <class T>
		static unique_ptr<BaseStatistics> PropagateStatistics(ClientContext &context, FunctionStatisticsInput &input) {
			return PropagateSimpleDatePartStatistics<0, 60>(input.child_stats);
		}
	};

	struct MinutesOperator {
		template <class TA, class TR>
		static inline TR Operation(TA input) {
			return 0;
		}

		template <class T>
		static unique_ptr<BaseStatistics> PropagateStatistics(ClientContext &context, FunctionStatisticsInput &input) {
			return PropagateSimpleDatePartStatistics<0, 60>(input.child_stats);
		}
	};

	struct HoursOperator {
		template <class TA, class TR>
		static inline TR Operation(TA input) {
			return 0;
		}

		template <class T>
		static unique_ptr<BaseStatistics> PropagateStatistics(ClientContext &context, FunctionStatisticsInput &input) {
			return PropagateSimpleDatePartStatistics<0, 24>(input.child_stats);
		}
	};

	struct EpochOperator {
		template <class TA, class TR>
		static inline TR Operation(TA input) {
			return Date::Epoch(input);
		}

		template <class T>
		static unique_ptr<BaseStatistics> PropagateStatistics(ClientContext &context, FunctionStatisticsInput &input) {
			return PropagateDatePartStatistics<T, EpochOperator>(input.child_stats);
		}
	};

	struct EraOperator {
		template <class TR>
		static inline TR EraFromYear(TR yyyy) {
			return yyyy > 0 ? 1 : 0;
		}

		template <class TA, class TR>
		static inline TR Operation(TA input) {
			return EraFromYear(Date::ExtractYear(input));
		}

		template <class T>
		static unique_ptr<BaseStatistics> PropagateStatistics(ClientContext &context, FunctionStatisticsInput &input) {
			return PropagateSimpleDatePartStatistics<0, 1>(input.child_stats);
		}
	};

	struct TimezoneOperator {
		template <class TA, class TR>
		static inline TR Operation(TA input) {
			// Regular timestamps are UTC.
			return 0;
		}

		template <class T>
		static unique_ptr<BaseStatistics> PropagateStatistics(ClientContext &context, FunctionStatisticsInput &input) {
			return PropagateSimpleDatePartStatistics<0, 0>(input.child_stats);
		}
	};

	struct JulianDayOperator {
		template <class TA, class TR>
		static inline TR Operation(TA input) {
			return Timestamp::GetJulianDay(input);
		}

		template <class T>
		static unique_ptr<BaseStatistics> PropagateStatistics(ClientContext &context, FunctionStatisticsInput &input) {
			return PropagateDatePartStatistics<T, JulianDayOperator, double>(input.child_stats, LogicalType::DOUBLE);
		}
	};

	// These are all zero and have the same restrictions
	using TimezoneHourOperator = TimezoneOperator;
	using TimezoneMinuteOperator = TimezoneOperator;

	struct StructOperator {
		using part_codes_t = vector<DatePartSpecifier>;
		using part_mask_t = uint64_t;

		enum MaskBits : uint8_t {
			YMD = 1 << 0,
			DOW = 1 << 1,
			DOY = 1 << 2,
			EPOCH = 1 << 3,
			TIME = 1 << 4,
			ZONE = 1 << 5,
			ISO = 1 << 6,
			JD = 1 << 7
		};

		static part_mask_t GetMask(const part_codes_t &part_codes) {
			part_mask_t mask = 0;
			for (const auto &part_code : part_codes) {
				switch (part_code) {
				case DatePartSpecifier::YEAR:
				case DatePartSpecifier::MONTH:
				case DatePartSpecifier::DAY:
				case DatePartSpecifier::DECADE:
				case DatePartSpecifier::CENTURY:
				case DatePartSpecifier::MILLENNIUM:
				case DatePartSpecifier::QUARTER:
				case DatePartSpecifier::ERA:
					mask |= YMD;
					break;
				case DatePartSpecifier::YEARWEEK:
				case DatePartSpecifier::WEEK:
				case DatePartSpecifier::ISOYEAR:
					mask |= ISO;
					break;
				case DatePartSpecifier::DOW:
				case DatePartSpecifier::ISODOW:
					mask |= DOW;
					break;
				case DatePartSpecifier::DOY:
					mask |= DOY;
					break;
				case DatePartSpecifier::EPOCH:
					mask |= EPOCH;
					break;
				case DatePartSpecifier::JULIAN_DAY:
					mask |= JD;
					break;
				case DatePartSpecifier::MICROSECONDS:
				case DatePartSpecifier::MILLISECONDS:
				case DatePartSpecifier::SECOND:
				case DatePartSpecifier::MINUTE:
				case DatePartSpecifier::HOUR:
					mask |= TIME;
					break;
				case DatePartSpecifier::TIMEZONE:
				case DatePartSpecifier::TIMEZONE_HOUR:
				case DatePartSpecifier::TIMEZONE_MINUTE:
					mask |= ZONE;
					break;
				}
			}
			return mask;
		}

		template <typename P>
		static inline P HasPartValue(vector<P> part_values, DatePartSpecifier part) {
			static const auto BEGIN_DOUBLE = size_t(DatePartSpecifier::JULIAN_DAY);
			auto idx = size_t(part);
			if (IsBigintDatepart(part)) {
				return part_values[idx];
			} else {
				return part_values[idx - BEGIN_DOUBLE];
			}
		}

		using bigint_vec = vector<int64_t *>;
		using double_vec = vector<double *>;

		template <class TA>
		static inline void Operation(bigint_vec &bigint_values, double_vec &double_values, const TA &input,
		                             const idx_t idx, const part_mask_t mask) {
			int64_t *bigint_data;
			// YMD calculations
			int32_t yyyy = 1970;
			int32_t mm = 0;
			int32_t dd = 1;
			if (mask & YMD) {
				Date::Convert(input, yyyy, mm, dd);
				bigint_data = HasPartValue(bigint_values, DatePartSpecifier::YEAR);
				if (bigint_data) {
					bigint_data[idx] = yyyy;
				}
				bigint_data = HasPartValue(bigint_values, DatePartSpecifier::MONTH);
				if (bigint_data) {
					bigint_data[idx] = mm;
				}
				bigint_data = HasPartValue(bigint_values, DatePartSpecifier::DAY);
				if (bigint_data) {
					bigint_data[idx] = dd;
				}
				bigint_data = HasPartValue(bigint_values, DatePartSpecifier::DECADE);
				if (bigint_data) {
					bigint_data[idx] = DecadeOperator::DecadeFromYear(yyyy);
				}
				bigint_data = HasPartValue(bigint_values, DatePartSpecifier::CENTURY);
				if (bigint_data) {
					bigint_data[idx] = CenturyOperator::CenturyFromYear(yyyy);
				}
				bigint_data = HasPartValue(bigint_values, DatePartSpecifier::MILLENNIUM);
				if (bigint_data) {
					bigint_data[idx] = MillenniumOperator::MillenniumFromYear(yyyy);
				}
				bigint_data = HasPartValue(bigint_values, DatePartSpecifier::QUARTER);
				if (bigint_data) {
					bigint_data[idx] = QuarterOperator::QuarterFromMonth(mm);
				}
				bigint_data = HasPartValue(bigint_values, DatePartSpecifier::ERA);
				if (bigint_data) {
					bigint_data[idx] = EraOperator::EraFromYear(yyyy);
				}
			}

			// Week calculations
			if (mask & DOW) {
				auto isodow = Date::ExtractISODayOfTheWeek(input);
				bigint_data = HasPartValue(bigint_values, DatePartSpecifier::DOW);
				if (bigint_data) {
					bigint_data[idx] = DayOfWeekOperator::DayOfWeekFromISO(isodow);
				}
				bigint_data = HasPartValue(bigint_values, DatePartSpecifier::ISODOW);
				if (bigint_data) {
					bigint_data[idx] = isodow;
				}
			}

			// ISO calculations
			if (mask & ISO) {
				int32_t ww = 0;
				int32_t iyyy = 0;
				Date::ExtractISOYearWeek(input, iyyy, ww);
				bigint_data = HasPartValue(bigint_values, DatePartSpecifier::WEEK);
				if (bigint_data) {
					bigint_data[idx] = ww;
				}
				bigint_data = HasPartValue(bigint_values, DatePartSpecifier::ISOYEAR);
				if (bigint_data) {
					bigint_data[idx] = iyyy;
				}
				bigint_data = HasPartValue(bigint_values, DatePartSpecifier::YEARWEEK);
				if (bigint_data) {
					bigint_data[idx] = YearWeekOperator::YearWeekFromParts(iyyy, ww);
				}
			}

			if (mask & EPOCH) {
				bigint_data = HasPartValue(bigint_values, DatePartSpecifier::EPOCH);
				if (bigint_data) {
					bigint_data[idx] = Date::Epoch(input);
				}
			}
			if (mask & DOY) {
				bigint_data = HasPartValue(bigint_values, DatePartSpecifier::DOY);
				if (bigint_data) {
					bigint_data[idx] = Date::ExtractDayOfTheYear(input);
				}
			}
			if (mask & JD) {
				auto double_data = HasPartValue(double_values, DatePartSpecifier::JULIAN_DAY);
				if (double_data) {
					double_data[idx] = Date::ExtractJulianDay(input);
				}
			}
		}
	};
};

template <class T>
static void LastYearFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	int32_t last_year = 0;
	UnaryExecutor::ExecuteWithNulls<T, int64_t>(args.data[0], result, args.size(),
	                                            [&](T input, ValidityMask &mask, idx_t idx) {
		                                            if (Value::IsFinite(input)) {
			                                            return Date::ExtractYear(input, &last_year);
		                                            } else {
			                                            mask.SetInvalid(idx);
			                                            return 0;
		                                            }
	                                            });
}

template <>
int64_t DatePart::YearOperator::Operation(timestamp_t input) {
	return YearOperator::Operation<date_t, int64_t>(Timestamp::GetDate(input));
}

template <>
int64_t DatePart::YearOperator::Operation(interval_t input) {
	return input.months / Interval::MONTHS_PER_YEAR;
}

template <>
int64_t DatePart::YearOperator::Operation(dtime_t input) {
	throw NotImplementedException("\"time\" units \"year\" not recognized");
}

template <>
int64_t DatePart::MonthOperator::Operation(timestamp_t input) {
	return MonthOperator::Operation<date_t, int64_t>(Timestamp::GetDate(input));
}

template <>
int64_t DatePart::MonthOperator::Operation(interval_t input) {
	return input.months % Interval::MONTHS_PER_YEAR;
}

template <>
int64_t DatePart::MonthOperator::Operation(dtime_t input) {
	throw NotImplementedException("\"time\" units \"month\" not recognized");
}

template <>
int64_t DatePart::DayOperator::Operation(timestamp_t input) {
	return DayOperator::Operation<date_t, int64_t>(Timestamp::GetDate(input));
}

template <>
int64_t DatePart::DayOperator::Operation(interval_t input) {
	return input.days;
}

template <>
int64_t DatePart::DayOperator::Operation(dtime_t input) {
	throw NotImplementedException("\"time\" units \"day\" not recognized");
}

template <>
int64_t DatePart::DecadeOperator::Operation(interval_t input) {
	return input.months / Interval::MONTHS_PER_DECADE;
}

template <>
int64_t DatePart::DecadeOperator::Operation(dtime_t input) {
	throw NotImplementedException("\"time\" units \"decade\" not recognized");
}

template <>
int64_t DatePart::CenturyOperator::Operation(interval_t input) {
	return input.months / Interval::MONTHS_PER_CENTURY;
}

template <>
int64_t DatePart::CenturyOperator::Operation(dtime_t input) {
	throw NotImplementedException("\"time\" units \"century\" not recognized");
}

template <>
int64_t DatePart::MillenniumOperator::Operation(interval_t input) {
	return input.months / Interval::MONTHS_PER_MILLENIUM;
}

template <>
int64_t DatePart::MillenniumOperator::Operation(dtime_t input) {
	throw NotImplementedException("\"time\" units \"millennium\" not recognized");
}

template <>
int64_t DatePart::QuarterOperator::Operation(timestamp_t input) {
	return QuarterOperator::Operation<date_t, int64_t>(Timestamp::GetDate(input));
}

template <>
int64_t DatePart::QuarterOperator::Operation(interval_t input) {
	return MonthOperator::Operation<interval_t, int64_t>(input) / Interval::MONTHS_PER_QUARTER + 1;
}

template <>
int64_t DatePart::QuarterOperator::Operation(dtime_t input) {
	throw NotImplementedException("\"time\" units \"quarter\" not recognized");
}

template <>
int64_t DatePart::DayOfWeekOperator::Operation(timestamp_t input) {
	return DayOfWeekOperator::Operation<date_t, int64_t>(Timestamp::GetDate(input));
}

template <>
int64_t DatePart::DayOfWeekOperator::Operation(interval_t input) {
	throw NotImplementedException("interval units \"dow\" not recognized");
}

template <>
int64_t DatePart::DayOfWeekOperator::Operation(dtime_t input) {
	throw NotImplementedException("\"time\" units \"dow\" not recognized");
}

template <>
int64_t DatePart::ISODayOfWeekOperator::Operation(timestamp_t input) {
	return ISODayOfWeekOperator::Operation<date_t, int64_t>(Timestamp::GetDate(input));
}

template <>
int64_t DatePart::ISODayOfWeekOperator::Operation(interval_t input) {
	throw NotImplementedException("interval units \"isodow\" not recognized");
}

template <>
int64_t DatePart::ISODayOfWeekOperator::Operation(dtime_t input) {
	throw NotImplementedException("\"time\" units \"isodow\" not recognized");
}

template <>
int64_t DatePart::DayOfYearOperator::Operation(timestamp_t input) {
	return DayOfYearOperator::Operation<date_t, int64_t>(Timestamp::GetDate(input));
}

template <>
int64_t DatePart::DayOfYearOperator::Operation(interval_t input) {
	throw NotImplementedException("interval units \"doy\" not recognized");
}

template <>
int64_t DatePart::DayOfYearOperator::Operation(dtime_t input) {
	throw NotImplementedException("\"time\" units \"doy\" not recognized");
}

template <>
int64_t DatePart::WeekOperator::Operation(timestamp_t input) {
	return WeekOperator::Operation<date_t, int64_t>(Timestamp::GetDate(input));
}

template <>
int64_t DatePart::WeekOperator::Operation(interval_t input) {
	throw NotImplementedException("interval units \"week\" not recognized");
}

template <>
int64_t DatePart::WeekOperator::Operation(dtime_t input) {
	throw NotImplementedException("\"time\" units \"week\" not recognized");
}

template <>
int64_t DatePart::ISOYearOperator::Operation(timestamp_t input) {
	return ISOYearOperator::Operation<date_t, int64_t>(Timestamp::GetDate(input));
}

template <>
int64_t DatePart::ISOYearOperator::Operation(interval_t input) {
	throw NotImplementedException("interval units \"isoyear\" not recognized");
}

template <>
int64_t DatePart::ISOYearOperator::Operation(dtime_t input) {
	throw NotImplementedException("\"time\" units \"isoyear\" not recognized");
}

template <>
int64_t DatePart::YearWeekOperator::Operation(timestamp_t input) {
	return YearWeekOperator::Operation<date_t, int64_t>(Timestamp::GetDate(input));
}

template <>
int64_t DatePart::YearWeekOperator::Operation(interval_t input) {
	const auto yyyy = YearOperator::Operation<interval_t, int64_t>(input);
	const auto ww = WeekOperator::Operation<interval_t, int64_t>(input);
	return YearWeekOperator::YearWeekFromParts<int64_t>(yyyy, ww);
}

template <>
int64_t DatePart::YearWeekOperator::Operation(dtime_t input) {
	throw NotImplementedException("\"time\" units \"yearweek\" not recognized");
}

template <>
int64_t DatePart::EpochNanosecondsOperator::Operation(timestamp_t input) {
	return Timestamp::GetEpochNanoSeconds(input);
}

template <>
int64_t DatePart::EpochNanosecondsOperator::Operation(date_t input) {
	return Date::EpochNanoseconds(input);
}

template <>
int64_t DatePart::EpochNanosecondsOperator::Operation(interval_t input) {
	return Interval::GetNanoseconds(input);
}

template <>
int64_t DatePart::EpochMicrosecondsOperator::Operation(timestamp_t input) {
	return Timestamp::GetEpochMicroSeconds(input);
}

template <>
int64_t DatePart::EpochMicrosecondsOperator::Operation(date_t input) {
	return Date::EpochMicroseconds(input);
}

template <>
int64_t DatePart::EpochMicrosecondsOperator::Operation(interval_t input) {
	return Interval::GetMicro(input);
}

template <>
int64_t DatePart::EpochMillisOperator::Operation(timestamp_t input) {
	return Timestamp::GetEpochMs(input);
}

template <>
int64_t DatePart::EpochMillisOperator::Operation(date_t input) {
	return Date::EpochMilliseconds(input);
}

template <>
int64_t DatePart::EpochMillisOperator::Operation(interval_t input) {
	return Interval::GetMilli(input);
}

template <>
int64_t DatePart::MicrosecondsOperator::Operation(timestamp_t input) {
	auto time = Timestamp::GetTime(input);
	// remove everything but the second & microsecond part
	return time.micros % Interval::MICROS_PER_MINUTE;
}

template <>
int64_t DatePart::MicrosecondsOperator::Operation(interval_t input) {
	// remove everything but the second & microsecond part
	return input.micros % Interval::MICROS_PER_MINUTE;
}

template <>
int64_t DatePart::MicrosecondsOperator::Operation(dtime_t input) {
	// remove everything but the second & microsecond part
	return input.micros % Interval::MICROS_PER_MINUTE;
}

template <>
int64_t DatePart::MillisecondsOperator::Operation(timestamp_t input) {
	return MicrosecondsOperator::Operation<timestamp_t, int64_t>(input) / Interval::MICROS_PER_MSEC;
}

template <>
int64_t DatePart::MillisecondsOperator::Operation(interval_t input) {
	return MicrosecondsOperator::Operation<interval_t, int64_t>(input) / Interval::MICROS_PER_MSEC;
}

template <>
int64_t DatePart::MillisecondsOperator::Operation(dtime_t input) {
	return MicrosecondsOperator::Operation<dtime_t, int64_t>(input) / Interval::MICROS_PER_MSEC;
}

template <>
int64_t DatePart::SecondsOperator::Operation(timestamp_t input) {
	return MicrosecondsOperator::Operation<timestamp_t, int64_t>(input) / Interval::MICROS_PER_SEC;
}

template <>
int64_t DatePart::SecondsOperator::Operation(interval_t input) {
	return MicrosecondsOperator::Operation<interval_t, int64_t>(input) / Interval::MICROS_PER_SEC;
}

template <>
int64_t DatePart::SecondsOperator::Operation(dtime_t input) {
	return MicrosecondsOperator::Operation<dtime_t, int64_t>(input) / Interval::MICROS_PER_SEC;
}

template <>
int64_t DatePart::MinutesOperator::Operation(timestamp_t input) {
	auto time = Timestamp::GetTime(input);
	// remove the hour part, and truncate to minutes
	return (time.micros % Interval::MICROS_PER_HOUR) / Interval::MICROS_PER_MINUTE;
}

template <>
int64_t DatePart::MinutesOperator::Operation(interval_t input) {
	// remove the hour part, and truncate to minutes
	return (input.micros % Interval::MICROS_PER_HOUR) / Interval::MICROS_PER_MINUTE;
}

template <>
int64_t DatePart::MinutesOperator::Operation(dtime_t input) {
	// remove the hour part, and truncate to minutes
	return (input.micros % Interval::MICROS_PER_HOUR) / Interval::MICROS_PER_MINUTE;
}

template <>
int64_t DatePart::HoursOperator::Operation(timestamp_t input) {
	return Timestamp::GetTime(input).micros / Interval::MICROS_PER_HOUR;
}

template <>
int64_t DatePart::HoursOperator::Operation(interval_t input) {
	return input.micros / Interval::MICROS_PER_HOUR;
}

template <>
int64_t DatePart::HoursOperator::Operation(dtime_t input) {
	return input.micros / Interval::MICROS_PER_HOUR;
}

template <>
int64_t DatePart::EpochOperator::Operation(timestamp_t input) {
	return Timestamp::GetEpochSeconds(input);
}

template <>
int64_t DatePart::EpochOperator::Operation(interval_t input) {
	int64_t interval_years = input.months / Interval::MONTHS_PER_YEAR;
	int64_t interval_days;
	interval_days = Interval::DAYS_PER_YEAR * interval_years;
	interval_days += Interval::DAYS_PER_MONTH * (input.months % Interval::MONTHS_PER_YEAR);
	interval_days += input.days;
	int64_t interval_epoch;
	interval_epoch = interval_days * Interval::SECS_PER_DAY;
	// we add 0.25 days per year to sort of account for leap days
	interval_epoch += interval_years * (Interval::SECS_PER_DAY / 4);
	interval_epoch += input.micros / Interval::MICROS_PER_SEC;
	return interval_epoch;
}

template <>
int64_t DatePart::EpochOperator::Operation(dtime_t input) {
	return input.micros / Interval::MICROS_PER_SEC;
}

template <>
unique_ptr<BaseStatistics> DatePart::EpochOperator::PropagateStatistics<dtime_t>(ClientContext &context,
                                                                                 FunctionStatisticsInput &input) {
	// time seconds range over a single day
	return PropagateSimpleDatePartStatistics<0, 86400>(input.child_stats);
}

template <>
int64_t DatePart::EraOperator::Operation(timestamp_t input) {
	return EraOperator::Operation<date_t, int64_t>(Timestamp::GetDate(input));
}

template <>
int64_t DatePart::EraOperator::Operation(interval_t input) {
	throw NotImplementedException("interval units \"era\" not recognized");
}

template <>
int64_t DatePart::EraOperator::Operation(dtime_t input) {
	throw NotImplementedException("\"time\" units \"era\" not recognized");
}

template <>
int64_t DatePart::TimezoneOperator::Operation(date_t input) {
	throw NotImplementedException("\"date\" units \"timezone\" not recognized");
}

template <>
int64_t DatePart::TimezoneOperator::Operation(interval_t input) {
	throw NotImplementedException("\"interval\" units \"timezone\" not recognized");
}

template <>
int64_t DatePart::TimezoneOperator::Operation(dtime_t input) {
	return 0;
}

template <>
double DatePart::JulianDayOperator::Operation(date_t input) {
	return Date::ExtractJulianDay(input);
}

template <>
double DatePart::JulianDayOperator::Operation(interval_t input) {
	throw NotImplementedException("interval units \"julian\" not recognized");
}

template <>
double DatePart::JulianDayOperator::Operation(dtime_t input) {
	throw NotImplementedException("\"time\" units \"julian\" not recognized");
}

template <>
void DatePart::StructOperator::Operation(bigint_vec &bigint_values, double_vec &double_values, const dtime_t &input,
                                         const idx_t idx, const part_mask_t mask) {
	int64_t *part_data;
	if (mask & TIME) {
		const auto micros = MicrosecondsOperator::Operation<dtime_t, int64_t>(input);
		part_data = HasPartValue(bigint_values, DatePartSpecifier::MICROSECONDS);
		if (part_data) {
			part_data[idx] = micros;
		}
		part_data = HasPartValue(bigint_values, DatePartSpecifier::MILLISECONDS);
		if (part_data) {
			part_data[idx] = micros / Interval::MICROS_PER_MSEC;
		}
		part_data = HasPartValue(bigint_values, DatePartSpecifier::SECOND);
		if (part_data) {
			part_data[idx] = micros / Interval::MICROS_PER_SEC;
		}
		part_data = HasPartValue(bigint_values, DatePartSpecifier::MINUTE);
		if (part_data) {
			part_data[idx] = MinutesOperator::Operation<dtime_t, int64_t>(input);
		}
		part_data = HasPartValue(bigint_values, DatePartSpecifier::HOUR);
		if (part_data) {
			part_data[idx] = HoursOperator::Operation<dtime_t, int64_t>(input);
		}
	}

	if (mask & EPOCH) {
		part_data = HasPartValue(bigint_values, DatePartSpecifier::EPOCH);
		if (part_data) {
			part_data[idx] = EpochOperator::Operation<dtime_t, int64_t>(input);
			;
		}
	}

	if (mask & ZONE) {
		part_data = HasPartValue(bigint_values, DatePartSpecifier::TIMEZONE);
		if (part_data) {
			part_data[idx] = 0;
		}
		part_data = HasPartValue(bigint_values, DatePartSpecifier::TIMEZONE_HOUR);
		if (part_data) {
			part_data[idx] = 0;
		}
		part_data = HasPartValue(bigint_values, DatePartSpecifier::TIMEZONE_MINUTE);
		if (part_data) {
			part_data[idx] = 0;
		}
	}
}

template <>
void DatePart::StructOperator::Operation(bigint_vec &bigint_values, double_vec &double_values, const timestamp_t &input,
                                         const idx_t idx, const part_mask_t mask) {
	date_t d;
	dtime_t t;
	Timestamp::Convert(input, d, t);

	// Both define epoch, and the correct value is the sum.
	// So mask it out and compute it separately.
	Operation(bigint_values, double_values, d, idx, mask & ~EPOCH);
	Operation(bigint_values, double_values, t, idx, mask & ~EPOCH);

	if (mask & EPOCH) {
		auto part_data = HasPartValue(bigint_values, DatePartSpecifier::EPOCH);
		if (part_data) {
			part_data[idx] = EpochOperator::Operation<timestamp_t, int64_t>(input);
		}
	}

	if (mask & JD) {
		auto part_data = HasPartValue(double_values, DatePartSpecifier::JULIAN_DAY);
		if (part_data) {
			part_data[idx] = JulianDayOperator::Operation<timestamp_t, double>(input);
		}
	}
}

template <>
void DatePart::StructOperator::Operation(bigint_vec &bigint_values, double_vec &double_values, const interval_t &input,
                                         const idx_t idx, const part_mask_t mask) {
	int64_t *part_data;
	if (mask & YMD) {
		const auto mm = input.months % Interval::MONTHS_PER_YEAR;
		part_data = HasPartValue(bigint_values, DatePartSpecifier::YEAR);
		if (part_data) {
			part_data[idx] = input.months / Interval::MONTHS_PER_YEAR;
		}
		part_data = HasPartValue(bigint_values, DatePartSpecifier::MONTH);
		if (part_data) {
			part_data[idx] = mm;
		}
		part_data = HasPartValue(bigint_values, DatePartSpecifier::DAY);
		if (part_data) {
			part_data[idx] = input.days;
		}
		part_data = HasPartValue(bigint_values, DatePartSpecifier::DECADE);
		if (part_data) {
			part_data[idx] = input.months / Interval::MONTHS_PER_DECADE;
		}
		part_data = HasPartValue(bigint_values, DatePartSpecifier::CENTURY);
		if (part_data) {
			part_data[idx] = input.months / Interval::MONTHS_PER_CENTURY;
		}
		part_data = HasPartValue(bigint_values, DatePartSpecifier::MILLENNIUM);
		if (part_data) {
			part_data[idx] = input.months / Interval::MONTHS_PER_MILLENIUM;
		}
		part_data = HasPartValue(bigint_values, DatePartSpecifier::QUARTER);
		if (part_data) {
			part_data[idx] = mm / Interval::MONTHS_PER_QUARTER + 1;
		}
	}

	if (mask & TIME) {
		const auto micros = MicrosecondsOperator::Operation<interval_t, int64_t>(input);
		part_data = HasPartValue(bigint_values, DatePartSpecifier::MICROSECONDS);
		if (part_data) {
			part_data[idx] = micros;
		}
		part_data = HasPartValue(bigint_values, DatePartSpecifier::MILLISECONDS);
		if (part_data) {
			part_data[idx] = micros / Interval::MICROS_PER_MSEC;
		}
		part_data = HasPartValue(bigint_values, DatePartSpecifier::SECOND);
		if (part_data) {
			part_data[idx] = micros / Interval::MICROS_PER_SEC;
		}
		part_data = HasPartValue(bigint_values, DatePartSpecifier::MINUTE);
		if (part_data) {
			part_data[idx] = MinutesOperator::Operation<interval_t, int64_t>(input);
		}
		part_data = HasPartValue(bigint_values, DatePartSpecifier::HOUR);
		if (part_data) {
			part_data[idx] = HoursOperator::Operation<interval_t, int64_t>(input);
		}
	}

	if (mask & EPOCH) {
		part_data = HasPartValue(bigint_values, DatePartSpecifier::EPOCH);
		if (part_data) {
			part_data[idx] = EpochOperator::Operation<interval_t, int64_t>(input);
		}
	}
}

template <typename T>
static int64_t ExtractElement(DatePartSpecifier type, T element) {
	switch (type) {
	case DatePartSpecifier::YEAR:
		return DatePart::YearOperator::template Operation<T, int64_t>(element);
	case DatePartSpecifier::MONTH:
		return DatePart::MonthOperator::template Operation<T, int64_t>(element);
	case DatePartSpecifier::DAY:
		return DatePart::DayOperator::template Operation<T, int64_t>(element);
	case DatePartSpecifier::DECADE:
		return DatePart::DecadeOperator::template Operation<T, int64_t>(element);
	case DatePartSpecifier::CENTURY:
		return DatePart::CenturyOperator::template Operation<T, int64_t>(element);
	case DatePartSpecifier::MILLENNIUM:
		return DatePart::MillenniumOperator::template Operation<T, int64_t>(element);
	case DatePartSpecifier::QUARTER:
		return DatePart::QuarterOperator::template Operation<T, int64_t>(element);
	case DatePartSpecifier::DOW:
		return DatePart::DayOfWeekOperator::template Operation<T, int64_t>(element);
	case DatePartSpecifier::ISODOW:
		return DatePart::ISODayOfWeekOperator::template Operation<T, int64_t>(element);
	case DatePartSpecifier::DOY:
		return DatePart::DayOfYearOperator::template Operation<T, int64_t>(element);
	case DatePartSpecifier::WEEK:
		return DatePart::WeekOperator::template Operation<T, int64_t>(element);
	case DatePartSpecifier::ISOYEAR:
		return DatePart::ISOYearOperator::template Operation<T, int64_t>(element);
	case DatePartSpecifier::YEARWEEK:
		return DatePart::YearWeekOperator::template Operation<T, int64_t>(element);
	case DatePartSpecifier::EPOCH:
		return DatePart::EpochOperator::template Operation<T, int64_t>(element);
	case DatePartSpecifier::MICROSECONDS:
		return DatePart::MicrosecondsOperator::template Operation<T, int64_t>(element);
	case DatePartSpecifier::MILLISECONDS:
		return DatePart::MillisecondsOperator::template Operation<T, int64_t>(element);
	case DatePartSpecifier::SECOND:
		return DatePart::SecondsOperator::template Operation<T, int64_t>(element);
	case DatePartSpecifier::MINUTE:
		return DatePart::MinutesOperator::template Operation<T, int64_t>(element);
	case DatePartSpecifier::HOUR:
		return DatePart::HoursOperator::template Operation<T, int64_t>(element);
	case DatePartSpecifier::ERA:
		return DatePart::EraOperator::template Operation<T, int64_t>(element);
	case DatePartSpecifier::TIMEZONE:
	case DatePartSpecifier::TIMEZONE_HOUR:
	case DatePartSpecifier::TIMEZONE_MINUTE:
		return DatePart::TimezoneOperator::template Operation<T, int64_t>(element);
	default:
		throw NotImplementedException("Specifier type not implemented for DATEPART");
	}
}

template <typename T>
static void DatePartFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.ColumnCount() == 2);
	auto &spec_arg = args.data[0];
	auto &date_arg = args.data[1];

	BinaryExecutor::ExecuteWithNulls<string_t, T, int64_t>(
	    spec_arg, date_arg, result, args.size(), [&](string_t specifier, T date, ValidityMask &mask, idx_t idx) {
		    if (Value::IsFinite(date)) {
			    return ExtractElement<T>(GetDatePartSpecifier(specifier.GetString()), date);
		    } else {
			    mask.SetInvalid(idx);
			    return int64_t(0);
		    }
	    });
}

static unique_ptr<FunctionData> DatePartBind(ClientContext &context, ScalarFunction &bound_function,
                                             vector<unique_ptr<Expression>> &arguments) {
	//	If we are only looking for Julian Days for timestamps,
	//	then return doubles.
	if (arguments[0]->HasParameter() || !arguments[0]->IsFoldable()) {
		return nullptr;
	}

	Value part_value = ExpressionExecutor::EvaluateScalar(context, *arguments[0]);
	if (part_value.IsNull()) {
		return nullptr;
	}
	const auto part_name = part_value.ToString();
	switch (GetDatePartSpecifier(part_name)) {
	case DatePartSpecifier::JULIAN_DAY:
		arguments.erase(arguments.begin());
		bound_function.arguments.erase(bound_function.arguments.begin());
		bound_function.name = "julian";
		bound_function.return_type = LogicalType::DOUBLE;
		switch (arguments[0]->return_type.id()) {
		case LogicalType::TIMESTAMP:
			bound_function.function = DatePart::UnaryFunction<timestamp_t, double, DatePart::JulianDayOperator>;
			bound_function.statistics = DatePart::JulianDayOperator::template PropagateStatistics<timestamp_t>;
			break;
		case LogicalType::DATE:
			bound_function.function = DatePart::UnaryFunction<date_t, double, DatePart::JulianDayOperator>;
			bound_function.statistics = DatePart::JulianDayOperator::template PropagateStatistics<date_t>;
			break;
		default:
			throw BinderException("%s can only take DATE or TIMESTAMP arguments", bound_function.name);
		}
		break;
	default:
		break;
	}

	return nullptr;
}

ScalarFunctionSet GetGenericDatePartFunction(scalar_function_t date_func, scalar_function_t ts_func,
                                             scalar_function_t interval_func, function_statistics_t date_stats,
                                             function_statistics_t ts_stats) {
	ScalarFunctionSet operator_set;
	operator_set.AddFunction(
	    ScalarFunction({LogicalType::DATE}, LogicalType::BIGINT, std::move(date_func), nullptr, nullptr, date_stats));
	operator_set.AddFunction(
	    ScalarFunction({LogicalType::TIMESTAMP}, LogicalType::BIGINT, std::move(ts_func), nullptr, nullptr, ts_stats));
	operator_set.AddFunction(ScalarFunction({LogicalType::INTERVAL}, LogicalType::BIGINT, std::move(interval_func)));
	return operator_set;
}

template <class OP>
static ScalarFunctionSet GetDatePartFunction() {
	return GetGenericDatePartFunction(
	    DatePart::UnaryFunction<date_t, int64_t, OP>, DatePart::UnaryFunction<timestamp_t, int64_t, OP>,
	    ScalarFunction::UnaryFunction<interval_t, int64_t, OP>, OP::template PropagateStatistics<date_t>,
	    OP::template PropagateStatistics<timestamp_t>);
}

ScalarFunctionSet GetGenericTimePartFunction(scalar_function_t date_func, scalar_function_t ts_func,
                                             scalar_function_t interval_func, scalar_function_t time_func,
                                             function_statistics_t date_stats, function_statistics_t ts_stats,
                                             function_statistics_t time_stats) {
	ScalarFunctionSet operator_set;
	operator_set.AddFunction(
	    ScalarFunction({LogicalType::DATE}, LogicalType::BIGINT, std::move(date_func), nullptr, nullptr, date_stats));
	operator_set.AddFunction(
	    ScalarFunction({LogicalType::TIMESTAMP}, LogicalType::BIGINT, std::move(ts_func), nullptr, nullptr, ts_stats));
	operator_set.AddFunction(ScalarFunction({LogicalType::INTERVAL}, LogicalType::BIGINT, std::move(interval_func)));
	operator_set.AddFunction(
	    ScalarFunction({LogicalType::TIME}, LogicalType::BIGINT, std::move(time_func), nullptr, nullptr, time_stats));
	return operator_set;
}

template <class OP>
static ScalarFunctionSet GetTimePartFunction() {
	return GetGenericTimePartFunction(
	    DatePart::UnaryFunction<date_t, int64_t, OP>, DatePart::UnaryFunction<timestamp_t, int64_t, OP>,
	    ScalarFunction::UnaryFunction<interval_t, int64_t, OP>, ScalarFunction::UnaryFunction<dtime_t, int64_t, OP>,
	    OP::template PropagateStatistics<date_t>, OP::template PropagateStatistics<timestamp_t>,
	    OP::template PropagateStatistics<dtime_t>);
}

struct LastDayOperator {
	template <class TA, class TR>
	static inline TR Operation(TA input) {
		int32_t yyyy, mm, dd;
		Date::Convert(input, yyyy, mm, dd);
		yyyy += (mm / 12);
		mm %= 12;
		++mm;
		return Date::FromDate(yyyy, mm, 1) - 1;
	}
};

template <>
date_t LastDayOperator::Operation(timestamp_t input) {
	return LastDayOperator::Operation<date_t, date_t>(Timestamp::GetDate(input));
}

struct MonthNameOperator {
	template <class TA, class TR>
	static inline TR Operation(TA input) {
		return Date::MONTH_NAMES[DatePart::MonthOperator::Operation<TA, int64_t>(input) - 1];
	}
};

struct DayNameOperator {
	template <class TA, class TR>
	static inline TR Operation(TA input) {
		return Date::DAY_NAMES[DatePart::DayOfWeekOperator::Operation<TA, int64_t>(input)];
	}
};

struct StructDatePart {
	using part_codes_t = vector<DatePartSpecifier>;

	static const auto BEGIN_BIGINT = size_t(DatePartSpecifier::YEAR);
	static const auto BEGIN_DOUBLE = size_t(DatePartSpecifier::JULIAN_DAY);
	static const auto BEGIN_INVALID = size_t(DatePartSpecifier::JULIAN_DAY) + 1;

	struct BindData : public VariableReturnBindData {
		part_codes_t part_codes;

		explicit BindData(const LogicalType &stype, const part_codes_t &part_codes_p)
		    : VariableReturnBindData(stype), part_codes(part_codes_p) {
		}

		unique_ptr<FunctionData> Copy() const override {
			return make_uniq<BindData>(stype, part_codes);
		}
	};

	static unique_ptr<FunctionData> Bind(ClientContext &context, ScalarFunction &bound_function,
	                                     vector<unique_ptr<Expression>> &arguments) {
		// collect names and deconflict, construct return type
		if (arguments[0]->HasParameter()) {
			throw ParameterNotResolvedException();
		}
		if (!arguments[0]->IsFoldable()) {
			throw BinderException("%s can only take constant lists of part names", bound_function.name);
		}

		case_insensitive_set_t name_collision_set;
		child_list_t<LogicalType> struct_children;
		part_codes_t part_codes;

		Value parts_list = ExpressionExecutor::EvaluateScalar(context, *arguments[0]);
		if (parts_list.type().id() == LogicalTypeId::LIST) {
			auto &list_children = ListValue::GetChildren(parts_list);
			if (list_children.empty()) {
				throw BinderException("%s requires non-empty lists of part names", bound_function.name);
			}
			for (const auto &part_value : list_children) {
				if (part_value.IsNull()) {
					throw BinderException("NULL struct entry name in %s", bound_function.name);
				}
				const auto part_name = part_value.ToString();
				const auto part_code = GetDateTypePartSpecifier(part_name, arguments[1]->return_type);
				if (name_collision_set.find(part_name) != name_collision_set.end()) {
					throw BinderException("Duplicate struct entry name \"%s\" in %s", part_name, bound_function.name);
				}
				name_collision_set.insert(part_name);
				part_codes.emplace_back(part_code);
				const auto part_type = IsBigintDatepart(part_code) ? LogicalType::BIGINT : LogicalType::DOUBLE;
				struct_children.emplace_back(make_pair(part_name, part_type));
			}
		} else {
			throw BinderException("%s can only take constant lists of part names", bound_function.name);
		}

		Function::EraseArgument(bound_function, arguments, 0);
		bound_function.return_type = LogicalType::STRUCT(struct_children);
		return make_uniq<BindData>(bound_function.return_type, part_codes);
	}

	template <typename INPUT_TYPE>
	static void Function(DataChunk &args, ExpressionState &state, Vector &result) {
		auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
		auto &info = func_expr.bind_info->Cast<BindData>();
		D_ASSERT(args.ColumnCount() == 1);

		const auto count = args.size();
		Vector &input = args.data[0];
		DatePart::StructOperator::bigint_vec bigint_values(size_t(BEGIN_DOUBLE), nullptr);
		DatePart::StructOperator::double_vec double_values(BEGIN_INVALID - size_t(BEGIN_DOUBLE), nullptr);
		const auto part_mask = DatePart::StructOperator::GetMask(info.part_codes);

		auto &child_entries = StructVector::GetEntries(result);

		// The first computer of a part "owns" it
		// and other requestors just reference the owner
		vector<size_t> owners(int(DatePartSpecifier::JULIAN_DAY) + 1, child_entries.size());
		for (size_t col = 0; col < child_entries.size(); ++col) {
			const auto part_index = size_t(info.part_codes[col]);
			if (owners[part_index] == child_entries.size()) {
				owners[part_index] = col;
			}
		}

		if (input.GetVectorType() == VectorType::CONSTANT_VECTOR) {
			result.SetVectorType(VectorType::CONSTANT_VECTOR);

			if (ConstantVector::IsNull(input)) {
				ConstantVector::SetNull(result, true);
			} else {
				ConstantVector::SetNull(result, false);
				for (size_t col = 0; col < child_entries.size(); ++col) {
					auto &child_entry = child_entries[col];
					ConstantVector::SetNull(*child_entry, false);
					const auto part_index = size_t(info.part_codes[col]);
					if (owners[part_index] == col) {
						if (IsBigintDatepart(info.part_codes[col])) {
							bigint_values[part_index - BEGIN_BIGINT] = ConstantVector::GetData<int64_t>(*child_entry);
						} else {
							double_values[part_index - BEGIN_DOUBLE] = ConstantVector::GetData<double>(*child_entry);
						}
					}
				}
				auto tdata = ConstantVector::GetData<INPUT_TYPE>(input);
				if (Value::IsFinite(tdata[0])) {
					DatePart::StructOperator::Operation(bigint_values, double_values, tdata[0], 0, part_mask);
				} else {
					for (auto &child_entry : child_entries) {
						ConstantVector::SetNull(*child_entry, true);
					}
				}
			}
		} else {
			UnifiedVectorFormat rdata;
			input.ToUnifiedFormat(count, rdata);

			const auto &arg_valid = rdata.validity;
			auto tdata = UnifiedVectorFormat::GetData<INPUT_TYPE>(rdata);

			// Start with a valid flat vector
			result.SetVectorType(VectorType::FLAT_VECTOR);
			auto &res_valid = FlatVector::Validity(result);
			if (res_valid.GetData()) {
				res_valid.SetAllValid(count);
			}

			// Start with valid children
			for (size_t col = 0; col < child_entries.size(); ++col) {
				auto &child_entry = child_entries[col];
				child_entry->SetVectorType(VectorType::FLAT_VECTOR);
				auto &child_validity = FlatVector::Validity(*child_entry);
				if (child_validity.GetData()) {
					child_validity.SetAllValid(count);
				}

				// Pre-multiplex
				const auto part_index = size_t(info.part_codes[col]);
				if (owners[part_index] == col) {
					if (IsBigintDatepart(info.part_codes[col])) {
						bigint_values[part_index - BEGIN_BIGINT] = FlatVector::GetData<int64_t>(*child_entry);
					} else {
						double_values[part_index - BEGIN_DOUBLE] = FlatVector::GetData<double>(*child_entry);
					}
				}
			}

			for (idx_t i = 0; i < count; ++i) {
				const auto idx = rdata.sel->get_index(i);
				if (arg_valid.RowIsValid(idx)) {
					if (Value::IsFinite(tdata[idx])) {
						DatePart::StructOperator::Operation(bigint_values, double_values, tdata[idx], i, part_mask);
					} else {
						for (auto &child_entry : child_entries) {
							FlatVector::Validity(*child_entry).SetInvalid(i);
						}
					}
				} else {
					res_valid.SetInvalid(i);
					for (auto &child_entry : child_entries) {
						FlatVector::Validity(*child_entry).SetInvalid(i);
					}
				}
			}
		}

		// Reference any duplicate parts
		for (size_t col = 0; col < child_entries.size(); ++col) {
			const auto part_index = size_t(info.part_codes[col]);
			const auto owner = owners[part_index];
			if (owner != col) {
				child_entries[col]->Reference(*child_entries[owner]);
			}
		}

		result.Verify(count);
	}

	static void SerializeFunction(Serializer &serializer, const optional_ptr<FunctionData> bind_data_p,
	                              const ScalarFunction &function) {
		D_ASSERT(bind_data_p);
		auto &info = bind_data_p->Cast<BindData>();
		serializer.WriteProperty(100, "stype", info.stype);
		serializer.WriteProperty(101, "part_codes", info.part_codes);
	}

	static unique_ptr<FunctionData> DeserializeFunction(Deserializer &deserializer, ScalarFunction &bound_function) {
		auto stype = deserializer.ReadProperty<LogicalType>(100, "stype");
		auto part_codes = deserializer.ReadProperty<vector<DatePartSpecifier>>(101, "part_codes");
		return make_uniq<BindData>(std::move(stype), std::move(part_codes));
	}

	template <typename INPUT_TYPE>
	static ScalarFunction GetFunction(const LogicalType &temporal_type) {
		auto part_type = LogicalType::LIST(LogicalType::VARCHAR);
		auto result_type = LogicalType::STRUCT({});
		ScalarFunction result({part_type, temporal_type}, result_type, Function<INPUT_TYPE>, Bind);
		result.serialize = SerializeFunction;
		result.deserialize = DeserializeFunction;
		return result;
	}
};

ScalarFunctionSet YearFun::GetFunctions() {
	return GetGenericDatePartFunction(LastYearFunction<date_t>, LastYearFunction<timestamp_t>,
	                                  ScalarFunction::UnaryFunction<interval_t, int64_t, DatePart::YearOperator>,
	                                  DatePart::YearOperator::PropagateStatistics<date_t>,
	                                  DatePart::YearOperator::PropagateStatistics<timestamp_t>);
}

ScalarFunctionSet MonthFun::GetFunctions() {
	return GetDatePartFunction<DatePart::MonthOperator>();
}

ScalarFunctionSet DayFun::GetFunctions() {
	return GetDatePartFunction<DatePart::DayOperator>();
}

ScalarFunctionSet DecadeFun::GetFunctions() {
	return GetDatePartFunction<DatePart::DecadeOperator>();
}

ScalarFunctionSet CenturyFun::GetFunctions() {
	return GetDatePartFunction<DatePart::CenturyOperator>();
}

ScalarFunctionSet MillenniumFun::GetFunctions() {
	return GetDatePartFunction<DatePart::MillenniumOperator>();
}

ScalarFunctionSet QuarterFun::GetFunctions() {
	return GetDatePartFunction<DatePart::QuarterOperator>();
}

ScalarFunctionSet DayOfWeekFun::GetFunctions() {
	return GetDatePartFunction<DatePart::DayOfWeekOperator>();
}

ScalarFunctionSet ISODayOfWeekFun::GetFunctions() {
	return GetDatePartFunction<DatePart::ISODayOfWeekOperator>();
}

ScalarFunctionSet DayOfYearFun::GetFunctions() {
	return GetDatePartFunction<DatePart::DayOfYearOperator>();
}

ScalarFunctionSet WeekFun::GetFunctions() {
	return GetDatePartFunction<DatePart::WeekOperator>();
}

ScalarFunctionSet ISOYearFun::GetFunctions() {
	return GetDatePartFunction<DatePart::ISOYearOperator>();
}

ScalarFunctionSet EraFun::GetFunctions() {
	return GetDatePartFunction<DatePart::EraOperator>();
}

ScalarFunctionSet TimezoneFun::GetFunctions() {
	return GetDatePartFunction<DatePart::TimezoneOperator>();
}

ScalarFunctionSet TimezoneHourFun::GetFunctions() {
	return GetDatePartFunction<DatePart::TimezoneHourOperator>();
}

ScalarFunctionSet TimezoneMinuteFun::GetFunctions() {
	return GetDatePartFunction<DatePart::TimezoneMinuteOperator>();
}

ScalarFunctionSet EpochFun::GetFunctions() {
	return GetTimePartFunction<DatePart::EpochOperator>();
}

ScalarFunctionSet EpochNsFun::GetFunctions() {
	using OP = DatePart::EpochNanosecondsOperator;
	auto operator_set = GetTimePartFunction<OP>();

	//	TIMESTAMP WITH TIME ZONE has the same representation as TIMESTAMP so no need to defer to ICU
	auto tstz_func = DatePart::UnaryFunction<timestamp_t, int64_t, OP>;
	auto tstz_stats = OP::template PropagateStatistics<timestamp_t>;
	operator_set.AddFunction(
	    ScalarFunction({LogicalType::TIMESTAMP_TZ}, LogicalType::BIGINT, tstz_func, nullptr, nullptr, tstz_stats));
	return operator_set;
}

ScalarFunctionSet EpochUsFun::GetFunctions() {
	using OP = DatePart::EpochMicrosecondsOperator;
	auto operator_set = GetTimePartFunction<OP>();

	//	TIMESTAMP WITH TIME ZONE has the same representation as TIMESTAMP so no need to defer to ICU
	auto tstz_func = DatePart::UnaryFunction<timestamp_t, int64_t, OP>;
	auto tstz_stats = OP::template PropagateStatistics<timestamp_t>;
	operator_set.AddFunction(
	    ScalarFunction({LogicalType::TIMESTAMP_TZ}, LogicalType::BIGINT, tstz_func, nullptr, nullptr, tstz_stats));
	return operator_set;
}

ScalarFunctionSet EpochMsFun::GetFunctions() {
	using OP = DatePart::EpochMillisOperator;
	auto operator_set = GetTimePartFunction<OP>();

	//	TIMESTAMP WITH TIME ZONE has the same representation as TIMESTAMP so no need to defer to ICU
	auto tstz_func = DatePart::UnaryFunction<timestamp_t, int64_t, OP>;
	auto tstz_stats = OP::template PropagateStatistics<timestamp_t>;
	operator_set.AddFunction(
	    ScalarFunction({LogicalType::TIMESTAMP_TZ}, LogicalType::BIGINT, tstz_func, nullptr, nullptr, tstz_stats));

	//	Legacy inverse BIGINT => TIMESTAMP
	operator_set.AddFunction(
	    ScalarFunction({LogicalType::BIGINT}, LogicalType::TIMESTAMP, DatePart::EpochMillisOperator::Inverse));

	return operator_set;
}

ScalarFunctionSet MicrosecondsFun::GetFunctions() {
	return GetTimePartFunction<DatePart::MicrosecondsOperator>();
}

ScalarFunctionSet MillisecondsFun::GetFunctions() {
	return GetTimePartFunction<DatePart::MillisecondsOperator>();
}

ScalarFunctionSet SecondsFun::GetFunctions() {
	return GetTimePartFunction<DatePart::SecondsOperator>();
}

ScalarFunctionSet MinutesFun::GetFunctions() {
	return GetTimePartFunction<DatePart::MinutesOperator>();
}

ScalarFunctionSet HoursFun::GetFunctions() {
	return GetTimePartFunction<DatePart::HoursOperator>();
}

ScalarFunctionSet YearWeekFun::GetFunctions() {
	return GetDatePartFunction<DatePart::YearWeekOperator>();
}

ScalarFunctionSet DayOfMonthFun::GetFunctions() {
	return GetDatePartFunction<DatePart::DayOperator>();
}

ScalarFunctionSet WeekDayFun::GetFunctions() {
	return GetDatePartFunction<DatePart::DayOfWeekOperator>();
}

ScalarFunctionSet WeekOfYearFun::GetFunctions() {
	return GetDatePartFunction<DatePart::WeekOperator>();
}

ScalarFunctionSet LastDayFun::GetFunctions() {
	ScalarFunctionSet last_day;
	last_day.AddFunction(ScalarFunction({LogicalType::DATE}, LogicalType::DATE,
	                                    DatePart::UnaryFunction<date_t, date_t, LastDayOperator>));
	last_day.AddFunction(ScalarFunction({LogicalType::TIMESTAMP}, LogicalType::DATE,
	                                    DatePart::UnaryFunction<timestamp_t, date_t, LastDayOperator>));
	return last_day;
}

ScalarFunctionSet MonthNameFun::GetFunctions() {
	ScalarFunctionSet monthname;
	monthname.AddFunction(ScalarFunction({LogicalType::DATE}, LogicalType::VARCHAR,
	                                     DatePart::UnaryFunction<date_t, string_t, MonthNameOperator>));
	monthname.AddFunction(ScalarFunction({LogicalType::TIMESTAMP}, LogicalType::VARCHAR,
	                                     DatePart::UnaryFunction<timestamp_t, string_t, MonthNameOperator>));
	return monthname;
}

ScalarFunctionSet DayNameFun::GetFunctions() {
	ScalarFunctionSet dayname;
	dayname.AddFunction(ScalarFunction({LogicalType::DATE}, LogicalType::VARCHAR,
	                                   DatePart::UnaryFunction<date_t, string_t, DayNameOperator>));
	dayname.AddFunction(ScalarFunction({LogicalType::TIMESTAMP}, LogicalType::VARCHAR,
	                                   DatePart::UnaryFunction<timestamp_t, string_t, DayNameOperator>));
	return dayname;
}

ScalarFunctionSet JulianDayFun::GetFunctions() {
	using OP = DatePart::JulianDayOperator;

	ScalarFunctionSet operator_set;
	auto date_func = DatePart::UnaryFunction<date_t, double, OP>;
	auto date_stats = OP::template PropagateStatistics<date_t>;
	operator_set.AddFunction(
	    ScalarFunction({LogicalType::DATE}, LogicalType::DOUBLE, date_func, nullptr, nullptr, date_stats));
	auto ts_func = DatePart::UnaryFunction<timestamp_t, double, OP>;
	auto ts_stats = OP::template PropagateStatistics<timestamp_t>;
	operator_set.AddFunction(
	    ScalarFunction({LogicalType::TIMESTAMP}, LogicalType::DOUBLE, ts_func, nullptr, nullptr, ts_stats));

	return operator_set;
}

ScalarFunctionSet DatePartFun::GetFunctions() {
	ScalarFunctionSet date_part;
	date_part.AddFunction(ScalarFunction({LogicalType::VARCHAR, LogicalType::DATE}, LogicalType::BIGINT,
	                                     DatePartFunction<date_t>, DatePartBind));
	date_part.AddFunction(ScalarFunction({LogicalType::VARCHAR, LogicalType::TIMESTAMP}, LogicalType::BIGINT,
	                                     DatePartFunction<timestamp_t>, DatePartBind));
	date_part.AddFunction(ScalarFunction({LogicalType::VARCHAR, LogicalType::TIME}, LogicalType::BIGINT,
	                                     DatePartFunction<dtime_t>, DatePartBind));
	date_part.AddFunction(ScalarFunction({LogicalType::VARCHAR, LogicalType::INTERVAL}, LogicalType::BIGINT,
	                                     DatePartFunction<interval_t>, DatePartBind));

	// struct variants
	date_part.AddFunction(StructDatePart::GetFunction<date_t>(LogicalType::DATE));
	date_part.AddFunction(StructDatePart::GetFunction<timestamp_t>(LogicalType::TIMESTAMP));
	date_part.AddFunction(StructDatePart::GetFunction<dtime_t>(LogicalType::TIME));
	date_part.AddFunction(StructDatePart::GetFunction<interval_t>(LogicalType::INTERVAL));

	return date_part;
}

} // namespace duckdb
