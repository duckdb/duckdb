#include "core_functions/scalar/string_functions.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/hugeint.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/to_string.hpp"

namespace duckdb {

template <int64_t MULTIPLIER>
static void FormatBytesFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &heap = StringVector::GetStringHeap(result);
	UnaryExecutor::Execute<int64_t, string_t>(args.data[0], result, [&](int64_t bytes) {
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

template <int64_t MULTIPLIER>
static void FormatBytesHugeintFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &heap = StringVector::GetStringHeap(result);
	UnaryExecutor::Execute<hugeint_t, string_t>(args.data[0], result, [&](hugeint_t bytes) {
		bool is_negative = bytes < 0;
		hugeint_t unsigned_bytes;
		if (is_negative) {
			// |MIN| is one past the representable signed range; clamp to MAX to keep going
			// (the resulting display will be off by one byte, which is irrelevant for
			// human-readable output).
			if (!Hugeint::TryNegate(bytes, unsigned_bytes)) {
				unsigned_bytes = NumericLimits<hugeint_t>::Maximum();
			}
		} else {
			unsigned_bytes = bytes;
		}
		// If the absolute value fits in idx_t, defer to the existing formatter.
		idx_t small_bytes;
		if (Hugeint::TryCast<idx_t>(unsigned_bytes, small_bytes)) {
			return heap.AddString((is_negative ? "-" : "") +
			                      StringUtil::BytesToHumanReadableString(small_bytes, MULTIPLIER));
		}
		// Otherwise walk units in HUGEINT space, mirroring StringUtil::BytesToHumanReadableString
		// but extended to EiB/ZiB/YiB so we can represent values past UINT64_MAX.
		const char *unit[2][9] = {{"bytes", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB", "ZiB", "YiB"},
		                          {"bytes", "kB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB"}};
		const int sel = (MULTIPLIER == 1000);
		hugeint_t array[9];
		array[0] = unsigned_bytes;
		uint64_t remainders[9] = {};
		for (idx_t i = 1; i < 9; i++) {
			array[i] = Hugeint::DivModPositive(array[i - 1], uint64_t(MULTIPLIER), remainders[i - 1]);
		}
		for (idx_t i = 8; i >= 1; i--) {
			if (array[i] != 0) {
				// Map 0 -> 0 and (multiplier-1) -> 9
				uint64_t fractional_part = (remainders[i - 1] * 10) / uint64_t(MULTIPLIER);
				return heap.AddString(string(is_negative ? "-" : "") + array[i].ToString() + "." +
				                      to_string(fractional_part) + " " + unit[sel][i]);
			}
		}
		// Unreachable: if the value didn't fit in idx_t, at least the EiB slot is non-zero.
		return heap.AddString(string(is_negative ? "-" : "") + array[0].ToString() + " bytes");
	});
}

ScalarFunctionSet FormatBytesFun::GetFunctions() {
	ScalarFunctionSet set("format_bytes");
	set.AddFunction(ScalarFunction({LogicalType::BIGINT}, LogicalType::VARCHAR, FormatBytesFunction<1024>));
	set.AddFunction(ScalarFunction({LogicalType::HUGEINT}, LogicalType::VARCHAR, FormatBytesHugeintFunction<1024>));
	return set;
}

ScalarFunctionSet FormatreadabledecimalsizeFun::GetFunctions() {
	ScalarFunctionSet set("formatReadableDecimalSize");
	set.AddFunction(ScalarFunction({LogicalType::BIGINT}, LogicalType::VARCHAR, FormatBytesFunction<1000>));
	set.AddFunction(ScalarFunction({LogicalType::HUGEINT}, LogicalType::VARCHAR, FormatBytesHugeintFunction<1000>));
	return set;
}

} // namespace duckdb
