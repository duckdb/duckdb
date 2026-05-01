#include "core_functions/scalar/string_functions.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/string_heap.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/vector/string_vector.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/function/scalar_function.hpp"

namespace duckdb {
struct ExpressionState;
struct string_t;

template <int64_t MULTIPLIER>
static void FormatBytesFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &heap = StringVector::GetStringHeap(result);
	UnaryExecutor::Execute<int64_t, string_t>(args.data[0], result, args.size(), [&](int64_t bytes) {
		bool is_negative = bytes < 0;
		idx_t unsigned_bytes;
		if (bytes < 0) {
			if (bytes == NumericLimits<int64_t>::Minimum()) {
				unsigned_bytes = idx_t(NumericLimits<int64_t>::Maximum()) + 1;
			} else {
				unsigned_bytes = idx_t(-bytes);
			}
		} else {
			unsigned_bytes = idx_t(bytes);
		}
		return heap.AddString((is_negative ? "-" : "") +
		                      StringUtil::BytesToHumanReadableString(unsigned_bytes, MULTIPLIER));
	});
}

ScalarFunction FormatBytesFun::GetFunction() {
	return ScalarFunction({LogicalType::BIGINT}, LogicalType::VARCHAR, FormatBytesFunction<1024>);
}

ScalarFunction FormatreadabledecimalsizeFun::GetFunction() {
	return ScalarFunction({LogicalType::BIGINT}, LogicalType::VARCHAR, FormatBytesFunction<1000>);
}

} // namespace duckdb
