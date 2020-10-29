#include "duckdb/function/scalar/string_functions.hpp"

#include "utf8proc.hpp"

using namespace std;

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

static void strip_accents_function(DataChunk &args, ExpressionState &state, Vector &result) {
	assert(args.column_count() == 1);

	UnaryExecutor::Execute<string_t, string_t, true>(args.data[0], result, args.size(), [&](string_t input) {
		if (StripAccentsFun::IsAscii(input.GetDataUnsafe(), input.GetSize())) {
			return input;
		}

		auto c_str = input.GetTerminatedData();
		// non-ascii, perform collation
		// TODO patch utf8proc_remove_accents to require length
		auto stripped = utf8proc_remove_accents((const utf8proc_uint8_t *)c_str.get());
		auto result_str = StringVector::AddString(result, (const char *)stripped);
		free(stripped);
		return result_str;
	});
	StringVector::AddHeapReference(result, args.data[0]);
}

ScalarFunction StripAccentsFun::GetFunction() {
	return ScalarFunction("strip_accents", {LogicalType::VARCHAR}, LogicalType::VARCHAR, strip_accents_function);
}

void StripAccentsFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(StripAccentsFun::GetFunction());
}

} // namespace duckdb
