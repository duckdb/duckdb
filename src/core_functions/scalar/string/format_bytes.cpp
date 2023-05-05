#include "duckdb/core_functions/scalar/string_functions.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

static void FormatBytesFunction(DataChunk &args, ExpressionState &state, Vector &result) {
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
		return StringVector::AddString(result, (is_negative ? "-" : "") +
		                                           StringUtil::BytesToHumanReadableString(unsigned_bytes));
	});
}

ScalarFunction FormatBytesFun::GetFunction() {
	return ScalarFunction({LogicalType::BIGINT}, LogicalType::VARCHAR, FormatBytesFunction);
}

} // namespace duckdb
