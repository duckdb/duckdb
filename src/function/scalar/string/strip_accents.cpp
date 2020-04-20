#include "duckdb/function/scalar/string_functions.hpp"

#include "utf8proc.hpp"

using namespace std;

namespace duckdb {

static bool is_ascii(const char *input, idx_t n) {
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
	assert(args.data[0].type == TypeId::VARCHAR);

	UnaryExecutor::Execute<string_t, string_t, true>(args.data[0], result, args.size(), [&](string_t input) {
		auto input_data = input.GetData();
		auto input_length = input.GetSize();
		if (is_ascii(input_data, input_length)) {
			return input;
		}
		// non-ascii, perform collation
		auto stripped = utf8proc_remove_accents((const utf8proc_uint8_t *) input_data);
		auto result_str = StringVector::AddString(result, (const char*) stripped);
		free(stripped);
		return result_str;
	});
	StringVector::AddHeapReference(result, args.data[0]);
}

ScalarFunction StripAccentsFun::GetFunction() {
	return ScalarFunction("strip_accents", {SQLType::VARCHAR}, SQLType::VARCHAR, strip_accents_function);
}

void StripAccentsFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(StripAccentsFun::GetFunction());
}

} // namespace duckdb
