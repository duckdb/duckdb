#include "include/icu-datetrunc.hpp"
#include "include/icu-datefunc.hpp"

#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/vector_operations/senary_executor.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"

namespace duckdb {

struct ICUMakeTimestampTZFunc : public ICUDateFunc {
	template <typename T>
	static void Execute(DataChunk &input, ExpressionState &state, Vector &result) {
		auto &func_expr = (BoundFunctionExpression &)state.expr;
		auto &info = (BindData &)*func_expr.bind_info;
		CalendarPtr calendar_ptr(info.calendar->clone());
		auto calendar = calendar_ptr.get();

		SenaryExecutor::Execute<T, T, T, T, T, double, timestamp_t>(
		    input, result, [&](T yyyy, T mm, T dd, T hr, T mn, double ss) {
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
		    });
	}

	template <typename TA>
	static ScalarFunction GetFunction(const LogicalTypeId &type) {
		return ScalarFunction({type, type, type, type, type, LogicalType::DOUBLE}, LogicalType::TIMESTAMP_TZ,
		                      Execute<TA>, false, Bind);
	}

	static void AddFunction(const string &name, ClientContext &context) {
		ScalarFunctionSet set(name);
		set.AddFunction(GetFunction<int64_t>(LogicalType::BIGINT));

		CreateScalarFunctionInfo func_info(set);
		auto &catalog = Catalog::GetCatalog(context);
		catalog.AddFunction(context, &func_info);
	}
};

void RegisterICUMakeDateFunctions(ClientContext &context) {
	ICUMakeTimestampTZFunc::AddFunction("make_timestamptz", context);
}

} // namespace duckdb
