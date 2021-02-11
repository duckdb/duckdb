#include "duckdb/function/scalar/string_functions.hpp"

#include "utf8proc.hpp"

namespace duckdb {

bool StripAccentsFun::IsAscii(const char *input, idx_t n) {
	for (idx_t i = 0; i < n; i++) {
		if (input[i] & 0x80) {
			// non-ascii character
			return false;
		}
	}
	return true;
}

static void StripAccentsFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.ColumnCount() == 1);

	UnaryExecutor::Execute<string_t, string_t, true>(args.data[0], result, args.size(), [&](string_t input) {
		if (StripAccentsFun::IsAscii(input.GetDataUnsafe(), input.GetSize())) {
			return input;
		}

		// non-ascii, perform collation
		auto stripped = utf8proc_remove_accents((const utf8proc_uint8_t *)input.GetDataUnsafe(), input.GetSize());
		auto result_str = StringVector::AddString(result, (const char *)stripped);
		free(stripped);
		return result_str;
	});
	StringVector::AddHeapReference(result, args.data[0]);
}

ScalarFunction StripAccentsFun::GetFunction() {
	return ScalarFunction("strip_accents", {LogicalType::VARCHAR}, LogicalType::VARCHAR, StripAccentsFunction);
}

void StripAccentsFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(StripAccentsFun::GetFunction());
}

} // namespace duckdb
