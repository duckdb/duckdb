#include "duckdb/function/scalar/string_common.hpp"
#include "duckdb/function/scalar/string_functions.hpp"

#include "utf8proc.hpp"

namespace duckdb {

bool IsAscii(const char *input, idx_t n) {
	for (idx_t i = 0; i < n; i++) {
		if (input[i] & 0x80) {
			// non-ascii character
			return false;
		}
	}
	return true;
}

struct StripAccentsOperator {
	template <class INPUT_TYPE, class RESULT_TYPE>
	static RESULT_TYPE Operation(INPUT_TYPE input, Vector &result) {
		if (IsAscii(input.GetData(), input.GetSize())) {
			return input;
		}

		// non-ascii, perform collation
		auto stripped = utf8proc_remove_accents((const utf8proc_uint8_t *)input.GetData(),
		                                        UnsafeNumericCast<utf8proc_ssize_t>(input.GetSize()));
		auto result_str = StringVector::AddString(result, const_char_ptr_cast(stripped));
		free(stripped);
		return result_str;
	}
};

static void StripAccentsFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.ColumnCount() == 1);

	UnaryExecutor::ExecuteString<string_t, string_t, StripAccentsOperator>(args.data[0], result, args.size());
	StringVector::AddHeapReference(result, args.data[0]);
}

ScalarFunction StripAccentsFun::GetFunction() {
	return ScalarFunction("strip_accents", {LogicalType::VARCHAR}, LogicalType::VARCHAR, StripAccentsFunction);
}

} // namespace duckdb
