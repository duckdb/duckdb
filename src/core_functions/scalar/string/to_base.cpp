#include "duckdb/core_functions/scalar/string_functions.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"

namespace duckdb {

static const char alphabet[] = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";

static unique_ptr<FunctionData> ToBaseBind(ClientContext &context, ScalarFunction &bound_function,
                                           vector<unique_ptr<Expression>> &arguments) {
	// If no min_length is specified, default to 0
	D_ASSERT(arguments.size() == 2 || arguments.size() == 3);
	if (arguments.size() == 2) {
		arguments.push_back(make_uniq_base<Expression, BoundConstantExpression>(Value::INTEGER(0)));
	}
	return nullptr;
}

static void ToBaseFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &input = args.data[0];
	auto &radix = args.data[1];
	auto &min_length = args.data[2];
	auto count = args.size();

	TernaryExecutor::Execute<int64_t, int32_t, int32_t, string_t>(
	    input, radix, min_length, result, count, [&](int64_t input, int32_t radix, int32_t min_length) {
		    if (input < 0) {
			    throw InvalidInputException("'to_base' number must be greater than or equal to 0");
		    }
		    if (radix < 2 || radix > 36) {
			    throw InvalidInputException("'to_base' radix must be between 2 and 36");
		    }
		    if (min_length > 64 || min_length < 0) {
			    throw InvalidInputException("'to_base' min_length must be between 0 and 64");
		    }

		    char buf[64];
		    char *end = buf + sizeof(buf);
		    char *ptr = end;
		    do {
			    *--ptr = alphabet[input % radix];
			    input /= radix;
		    } while (input > 0);

		    auto length = end - ptr;
		    while (length < min_length) {
			    *--ptr = '0';
			    length++;
		    }

		    return StringVector::AddString(result, ptr, UnsafeNumericCast<idx_t>(end - ptr));
	    });
}

ScalarFunctionSet ToBaseFun::GetFunctions() {
	ScalarFunctionSet set("to_base");

	set.AddFunction(
	    ScalarFunction({LogicalType::BIGINT, LogicalType::INTEGER}, LogicalType::VARCHAR, ToBaseFunction, ToBaseBind));
	set.AddFunction(ScalarFunction({LogicalType::BIGINT, LogicalType::INTEGER, LogicalType::INTEGER},
	                               LogicalType::VARCHAR, ToBaseFunction, ToBaseBind));

	return set;
}

} // namespace duckdb
