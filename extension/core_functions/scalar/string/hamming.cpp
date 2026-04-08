#include <stdint.h>
#include <memory>

#include "core_functions/scalar/string_functions.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/string_type.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/vector_operations/binary_executor.hpp"
#include "duckdb/function/scalar_function.hpp"

namespace duckdb {
struct ExpressionState;

static int64_t MismatchesScalarFunction(Vector &result, const string_t str, string_t tgt) {
	idx_t str_len = str.GetSize();
	idx_t tgt_len = tgt.GetSize();

	if (str_len != tgt_len) {
		throw InvalidInputException("Mismatch Function: Strings must be of equal length!");
	}
	if (str_len < 1) {
		throw InvalidInputException("Mismatch Function: Strings must be of length > 0!");
	}

	idx_t mismatches = 0;
	auto str_str = str.GetData();
	auto tgt_str = tgt.GetData();

	for (idx_t idx = 0; idx < str_len; ++idx) {
		if (str_str[idx] != tgt_str[idx]) {
			mismatches++;
		}
	}
	return (int64_t)mismatches;
}

static void MismatchesFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &str_vec = args.data[0];
	auto &tgt_vec = args.data[1];

	BinaryExecutor::Execute<string_t, string_t, int64_t>(
	    str_vec, tgt_vec, result, args.size(),
	    [&](string_t str, string_t tgt) { return MismatchesScalarFunction(result, str, tgt); });
}

ScalarFunction HammingFun::GetFunction() {
	return ScalarFunction({LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::BIGINT, MismatchesFunction);
}

} // namespace duckdb
