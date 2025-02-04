#include "duckdb/common/operator/add.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/operator/subtract.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/vector_operations/senary_executor.hpp"
#include "duckdb/common/vector_operations/septenary_executor.hpp"
#include "duckdb/function/cast/cast_function_set.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "include/icu-casts.hpp"
#include "include/icu-datefunc.hpp"
#include "include/icu-datetrunc.hpp"

#include <cmath>

namespace duckdb {

date_t ICUMakeDate::Operation(icu::Calendar *calendar, timestamp_t instant) {
	if (!Timestamp::IsFinite(instant)) {
		return Timestamp::GetDate(instant);
	}

	// Extract the time zone parts
	SetTime(calendar, instant);
	const auto era = ExtractField(calendar, UCAL_ERA);
	const auto year = ExtractField(calendar, UCAL_YEAR);
	const auto mm = ExtractField(calendar, UCAL_MONTH) + 1;
	const auto dd = ExtractField(calendar, UCAL_DATE);

	const auto yyyy = era ? year : (-year + 1);
	date_t result;
	if (!Date::TryFromDate(yyyy, mm, dd, result)) {
		throw ConversionException("Unable to convert TIMESTAMPTZ to DATE");
	}

	return result;
}

date_t ICUMakeDate::ToDate(ClientContext &context, timestamp_t instant) {
	ICUDateFunc::BindData data(context);
	return Operation(data.calendar.get(), instant);
}

bool ICUMakeDate::CastToDate(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	auto &cast_data = parameters.cast_data->Cast<CastData>();
	auto &info = cast_data.info->Cast<BindData>();
	CalendarPtr calendar(info.calendar->clone());

	UnaryExecutor::Execute<timestamp_t, date_t>(source, result, count,
	                                            [&](timestamp_t input) { return Operation(calendar.get(), input); });
	return true;
}

BoundCastInfo ICUMakeDate::BindCastToDate(BindCastInput &input, const LogicalType &source, const LogicalType &target) {
	if (!input.context) {
		throw InternalException("Missing context for TIMESTAMPTZ to DATE cast.");
	}

	auto cast_data = make_uniq<CastData>(make_uniq<BindData>(*input.context));

	return BoundCastInfo(CastToDate, std::move(cast_data));
}

void ICUMakeDate::AddCasts(DatabaseInstance &db) {
	auto &config = DBConfig::GetConfig(db);
	auto &casts = config.GetCastFunctions();

	casts.RegisterCastFunction(LogicalType::TIMESTAMP_TZ, LogicalType::DATE, BindCastToDate);
}

struct ICUMakeTimestampTZFunc : public ICUDateFunc {
	template <typename T>
	static inline timestamp_t Operation(icu::Calendar *calendar, T yyyy, T mm, T dd, T hr, T mn, double ss) {
		const auto year = Cast::Operation<T, int32_t>(AddOperator::Operation<T, T, T>(yyyy, (yyyy < 0)));
		const auto month = Cast::Operation<T, int32_t>(SubtractOperatorOverflowCheck::Operation<T, T, T>(mm, 1));
		const auto day = Cast::Operation<T, int32_t>(dd);
		const auto hour = Cast::Operation<T, int32_t>(hr);
		const auto min = Cast::Operation<T, int32_t>(mn);

		const auto secs = Cast::Operation<double, int32_t>(ss);
		ss -= secs;
		ss *= Interval::MSECS_PER_SEC;
		const auto millis = int32_t(ss);
		int64_t micros = std::round((ss - millis) * Interval::MICROS_PER_MSEC);

		calendar->set(UCAL_YEAR, year);
		calendar->set(UCAL_MONTH, month);
		calendar->set(UCAL_DATE, day);
		calendar->set(UCAL_HOUR_OF_DAY, hour);
		calendar->set(UCAL_MINUTE, min);
		calendar->set(UCAL_SECOND, secs);
		calendar->set(UCAL_MILLISECOND, millis);

		return GetTime(calendar, micros);
	}

	template <typename T>
	static void FromMicros(DataChunk &input, ExpressionState &state, Vector &result) {
		UnaryExecutor::Execute<T, timestamp_t>(input.data[0], result, input.size(), [&](T micros) {
			const auto result = timestamp_t(micros);
			if (!Timestamp::IsFinite(result)) {
				throw ConversionException("Timestamp microseconds out of range: %ld", micros);
			}
			return result;
		});
	}

	template <typename T>
	static void Execute(DataChunk &input, ExpressionState &state, Vector &result) {
		auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
		auto &info = func_expr.bind_info->Cast<BindData>();
		CalendarPtr calendar_ptr(info.calendar->clone());
		auto calendar = calendar_ptr.get();

		// Three cases: no TZ, constant TZ, variable TZ
		if (input.ColumnCount() == SenaryExecutor::NCOLS) {
			SenaryExecutor::Execute<T, T, T, T, T, double, timestamp_t>(
			    input, result, [&](T yyyy, T mm, T dd, T hr, T mn, double ss) {
				    return Operation<T>(calendar, yyyy, mm, dd, hr, mn, ss);
			    });
		} else {
			D_ASSERT(input.ColumnCount() == SeptenaryExecutor::NCOLS);
			auto &tz_vec = input.data.back();
			if (tz_vec.GetVectorType() == VectorType::CONSTANT_VECTOR) {
				if (ConstantVector::IsNull(tz_vec)) {
					result.SetVectorType(VectorType::CONSTANT_VECTOR);
					ConstantVector::SetNull(result, true);
				} else {
					SetTimeZone(calendar, *ConstantVector::GetData<string_t>(tz_vec));
					SenaryExecutor::Execute<T, T, T, T, T, double, timestamp_t>(
					    input, result, [&](T yyyy, T mm, T dd, T hr, T mn, double ss) {
						    return Operation<T>(calendar, yyyy, mm, dd, hr, mn, ss);
					    });
				}
			} else {
				SeptenaryExecutor::Execute<T, T, T, T, T, double, string_t, timestamp_t>(
				    input, result, [&](T yyyy, T mm, T dd, T hr, T mn, double ss, string_t tz_id) {
					    SetTimeZone(calendar, tz_id);
					    return Operation<T>(calendar, yyyy, mm, dd, hr, mn, ss);
				    });
			}
		}
	}

	template <typename TA>
	static ScalarFunction GetSenaryFunction(const LogicalTypeId &type) {
		ScalarFunction function({type, type, type, type, type, LogicalType::DOUBLE}, LogicalType::TIMESTAMP_TZ,
		                        Execute<TA>, Bind);
		BaseScalarFunction::SetReturnsError(function);
		return function;
	}

	template <typename TA>
	static ScalarFunction GetSeptenaryFunction(const LogicalTypeId &type) {
		ScalarFunction function({type, type, type, type, type, LogicalType::DOUBLE, LogicalType::VARCHAR},
		                        LogicalType::TIMESTAMP_TZ, Execute<TA>, Bind);
		BaseScalarFunction::SetReturnsError(function);
		return function;
	}

	static void AddFunction(const string &name, DatabaseInstance &db) {
		ScalarFunctionSet set(name);
		set.AddFunction(GetSenaryFunction<int64_t>(LogicalType::BIGINT));
		set.AddFunction(GetSeptenaryFunction<int64_t>(LogicalType::BIGINT));
		ScalarFunction function({LogicalType::BIGINT}, LogicalType::TIMESTAMP_TZ, FromMicros<int64_t>);
		BaseScalarFunction::SetReturnsError(function);
		set.AddFunction(function);
		ExtensionUtil::RegisterFunction(db, set);
	}
};

void RegisterICUMakeDateFunctions(DatabaseInstance &db) {
	ICUMakeTimestampTZFunc::AddFunction("make_timestamptz", db);
	ICUMakeDate::AddCasts(db);
}

} // namespace duckdb
