#include "duckdb/function/scalar/date_functions.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

using namespace std;

namespace duckdb {

static void year_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count, BoundFunctionExpression &expr,
                          Vector &result) {
	assert(input_count == 1);
	auto &input = inputs[0];
	assert(input.type == TypeId::INTEGER || input.type == TypeId::BIGINT);

	result.Initialize(TypeId::INTEGER);
	result.nullmask = input.nullmask;
	result.count = input.count;
	result.sel_vector = input.sel_vector;
	auto result_data = (int *)result.data;
	switch (input.type) {
	case TypeId::INTEGER:
		VectorOperations::ExecType<date_t>(
		    input, [&](date_t date, index_t i, index_t k) { result_data[i] = Date::ExtractYear(date); });
		break;
	case TypeId::BIGINT:
		VectorOperations::ExecType<timestamp_t>(input, [&](timestamp_t timestamp, index_t i, index_t k) {
			result_data[i] = Date::ExtractYear(Timestamp::GetDate(timestamp));
		});
		break;
	default:
		throw NotImplementedException("Can only extract year from dates or timestamps");
	}
}

void YearFun::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunctionSet year("year");
	year.AddFunction(ScalarFunction({SQLType::DATE}, SQLType::INTEGER, year_function));
	year.AddFunction(ScalarFunction({SQLType::TIMESTAMP}, SQLType::INTEGER, year_function));
	set.AddFunction(year);
}

} // namespace duckdb
