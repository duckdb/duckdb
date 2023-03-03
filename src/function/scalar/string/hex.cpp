#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/function/scalar/string_functions.hpp"

#include <string.h>

namespace duckdb {

struct ToHexOperator {
	template <class INPUT_TYPE, class RESULT_TYPE>
	static RESULT_TYPE Operation(INPUT_TYPE input, Vector &result) {
		auto data = input.GetDataUnsafe();
		auto size = input.GetSize();

		// Allocate empty space
		auto target = StringVector::EmptyString(result, size * 2);
		auto output = target.GetDataWriteable();

		for (idx_t i = 0; i < size; ++i) {
			StringUtil::WriteByteToHex(data[i], output);
			output += 2;
		}

		target.Finalize();
		return target;
	}
};

/*
static void ToHexScalarFunction(string_t input_str, vector<char> &buffer) {
    buffer.clear();
    auto size = input_str.GetSize();
    auto raw_ptr = input_str.GetDataUnsafe();
    for (idx_t i = 0; i < size; ++i) {

    }
}
*/

/*
static void ToHexFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &input_vec = args.data[0];

    vector<char> buffer;
    UnaryExecutor::Execute<string_t, string_t>(
        input_vec, result, args.size(),
        [&](string_t input_string) {
            return StringVector::AddString(result,
                                           TranslateScalarFunction(input_string, needle_string, thread_string, buffer));
        });
}
*/

static void ToHexFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	UnaryExecutor::ExecuteString<string_t, string_t, ToHexOperator>(args.data[0], result, args.size());
}

void HexFun::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunctionSet to_hex("to_hex");

	to_hex.AddFunction(ScalarFunction({LogicalType::VARCHAR}, LogicalType::VARCHAR, ToHexFunction<true, false>));

	set.AddFunction(to_hex);
}

} // namespace duckdb
