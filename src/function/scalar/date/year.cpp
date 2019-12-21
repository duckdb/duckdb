#include "duckdb/function/scalar/date_functions.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

using namespace std;

namespace duckdb {

struct YearOperator {
	template <class TA, class TR> static inline TR Operation(TA input) {
		throw NotImplementedException("Unsupported type for year!");
	}
};

template <> int32_t YearOperator::Operation(date_t input) {
	return Date::ExtractYear(input);
}

template <> int32_t YearOperator::Operation(timestamp_t input) {
	return Date::ExtractYear(Timestamp::GetDate(input));
}

void YearFun::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunctionSet year("year");
	year.AddFunction(ScalarFunction({SQLType::DATE}, SQLType::INTEGER, ScalarFunction::UnaryFunction<date_t, int32_t, YearOperator>));
	year.AddFunction(ScalarFunction({SQLType::TIMESTAMP}, SQLType::INTEGER, ScalarFunction::UnaryFunction<timestamp_t, int32_t, YearOperator>));
	set.AddFunction(year);
}

} // namespace duckdb
