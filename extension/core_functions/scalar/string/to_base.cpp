#include "core_functions/scalar/string_functions.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/common/assert.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/string_heap.hpp"
#include "duckdb/common/types/string_type.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/vector/string_vector.hpp"
#include "duckdb/common/vector_operations/ternary_executor.hpp"
#include "duckdb/function/function.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/planner/expression.hpp"

namespace duckdb {
class ClientContext;
struct ExpressionState;

static const char alphabet[] = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";

static unique_ptr<FunctionData> ToBaseBind(BindScalarFunctionInput &input) {
	auto &arguments = input.GetArguments();
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

	auto &heap = StringVector::GetStringHeap(result);
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

		    return heap.AddString(ptr, UnsafeNumericCast<idx_t>(end - ptr));
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
