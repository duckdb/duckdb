#include "duckdb/function/scalar/string_functions.hpp"

#include "utf8proc_wrapper.hpp"

using namespace std;

namespace duckdb {

static void nfc_normalize_function(DataChunk &args, ExpressionState &state, Vector &result) {
	assert(args.column_count() == 1);

	UnaryExecutor::Execute<string_t, string_t, true>(args.data[0], result, args.size(), [&](string_t input) {
		auto input_data = input.GetDataUnsafe();
		auto input_length = input.GetSize();
		if (StripAccentsFun::IsAscii(input_data, input_length)) {
			return input;
		}
		auto normalized_str = Utf8Proc::Normalize(input_data, input_length);
		assert(normalized_str);
		auto result_str = StringVector::AddString(result, normalized_str);
		free(normalized_str);
		return result_str;
	});
	StringVector::AddHeapReference(result, args.data[0]);
}

ScalarFunction NFCNormalizeFun::GetFunction() {
	return ScalarFunction("nfc_normalize", {LogicalType::VARCHAR}, LogicalType::VARCHAR, nfc_normalize_function);
}

void NFCNormalizeFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(NFCNormalizeFun::GetFunction());
}

} // namespace duckdb
