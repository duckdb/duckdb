#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/vector_operations/senary_executor.hpp"
#include "duckdb/common/vector_operations/septenary_executor.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "include/icu-datefunc.hpp"
#include "include/icu-datetrunc.hpp"

namespace duckdb {

struct ICUMakeTimestampTZFunc : public ICUDateFunc {
	template <typename T>
	static inline timestamp_t Operation(icu::Calendar *calendar, T yyyy, T mm, T dd, T hr, T mn, double ss) {
		const auto year = yyyy + (yyyy < 0);

		const auto secs = int32_t(ss);
		ss -= secs;
		ss *= Interval::MSECS_PER_SEC;
		const auto millis = int32_t(ss);
		int64_t micros = std::round((ss - millis) * Interval::MICROS_PER_MSEC);

		calendar->set(UCAL_YEAR, int32_t(year));
		calendar->set(UCAL_MONTH, int32_t(mm - 1));
		calendar->set(UCAL_DATE, int32_t(dd));
		calendar->set(UCAL_HOUR_OF_DAY, int32_t(hr));
		calendar->set(UCAL_MINUTE, int32_t(mn));
		calendar->set(UCAL_SECOND, secs);
		calendar->set(UCAL_MILLISECOND, millis);

		return GetTime(calendar, micros);
	}

	template <typename T>
	static void Execute(DataChunk &input, ExpressionState &state, Vector &result) {
		auto &func_expr = (BoundFunctionExpression &)state.expr;
		auto &info = (BindData &)*func_expr.bind_info;
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
		return ScalarFunction({type, type, type, type, type, LogicalType::DOUBLE}, LogicalType::TIMESTAMP_TZ,
		                      Execute<TA>, false, false, Bind);
	}

	template <typename TA>
	static ScalarFunction GetSeptenaryFunction(const LogicalTypeId &type) {
		return ScalarFunction({type, type, type, type, type, LogicalType::DOUBLE, LogicalType::VARCHAR},
		                      LogicalType::TIMESTAMP_TZ, Execute<TA>, false, false, Bind);
	}

	static void AddFunction(const string &name, ClientContext &context) {
		ScalarFunctionSet set(name);
		set.AddFunction(GetSenaryFunction<int64_t>(LogicalType::BIGINT));
		set.AddFunction(GetSeptenaryFunction<int64_t>(LogicalType::BIGINT));

		CreateScalarFunctionInfo func_info(set);
		auto &catalog = Catalog::GetCatalog(context);
		catalog.AddFunction(context, &func_info);
	}
};

void RegisterICUMakeDateFunctions(ClientContext &context) {
	ICUMakeTimestampTZFunc::AddFunction("make_timestamptz", context);
}

} // namespace duckdb
