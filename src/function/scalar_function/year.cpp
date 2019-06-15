#include "function/scalar_function/year.hpp"

#include "common/exception.hpp"
#include "common/types/date.hpp"
#include "common/types/timestamp.hpp"
#include "common/vector_operations/vector_operations.hpp"

using namespace std;

namespace duckdb {

void year_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count, BoundFunctionExpression &expr,
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

bool year_matches_arguments(vector<SQLType> &arguments) {
	return arguments.size() == 1 && (arguments[0].id == SQLTypeId::DATE || arguments[0].id == SQLTypeId::TIMESTAMP);
}

SQLType year_get_return_type(vector<SQLType> &arguments) {
	return SQLType(SQLTypeId::INTEGER);
}

} // namespace duckdb
