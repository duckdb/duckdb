#include "duckdb/common/vector_operations/binary_executor.hpp"
#include "duckdb/core_functions/scalar/string_functions.hpp"
#include "duckdb/common/operator/multiply.hpp"

namespace duckdb {

static void RepeatFunction(DataChunk &args, ExpressionState &, Vector &result) {
	auto &str_vector = args.data[0];
	auto &cnt_vector = args.data[1];

	BinaryExecutor::Execute<string_t, int64_t, string_t>(
	    str_vector, cnt_vector, result, args.size(), [&](string_t str, int64_t cnt) {
		    auto input_str = str.GetData();
		    auto size_str = str.GetSize();
		    idx_t copy_count = cnt <= 0 || size_str == 0 ? 0 : UnsafeNumericCast<idx_t>(cnt);

		    idx_t copy_size;
		    if (TryMultiplyOperator::Operation(size_str, copy_count, copy_size)) {
			    auto result_str = StringVector::EmptyString(result, copy_size);
			    auto result_data = result_str.GetDataWriteable();
			    for (idx_t i = 0; i < copy_count; i++) {
				    memcpy(result_data + i * size_str, input_str, size_str);
			    }
			    result_str.Finalize();
			    return result_str;
		    } else {
			    throw OutOfRangeException(
			        "Cannot create a string of size: '%d' * '%d', the maximum supported string size is: '%d'", size_str,
			        copy_count, string_t::MAX_STRING_SIZE);
		    }
	    });
}

ScalarFunctionSet RepeatFun::GetFunctions() {
	ScalarFunctionSet repeat;
	for (const auto &type : {LogicalType::VARCHAR, LogicalType::BLOB}) {
		repeat.AddFunction(ScalarFunction({type, LogicalType::BIGINT}, type, RepeatFunction));
	}
	return repeat;
}

} // namespace duckdb
