#include "duckdb/function/scalar/string_common.hpp"
#include "duckdb/function/scalar/string_functions.hpp"
#include "utf8proc_wrapper.hpp"

namespace duckdb {

struct NFCNormalizeOperator {
	template <class INPUT_TYPE, class RESULT_TYPE>
	static RESULT_TYPE Operation(INPUT_TYPE input, Vector &result) {
		auto input_data = input.GetData();
		auto input_length = input.GetSize();
		if (IsAscii(input_data, input_length)) {
			return input;
		}
		auto normalized_str = Utf8Proc::Normalize(input_data, input_length);
		D_ASSERT(normalized_str);
		auto result_str = StringVector::AddString(result, normalized_str);
		free(normalized_str);
		return result_str;
	}
};

static void NFCNormalizeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.ColumnCount() == 1);

	UnaryExecutor::ExecuteString<string_t, string_t, NFCNormalizeOperator>(args.data[0], result, args.size());
	StringVector::AddHeapReference(result, args.data[0]);
}

ScalarFunction NFCNormalizeFun::GetFunction() {
	return ScalarFunction("nfc_normalize", {LogicalType::VARCHAR}, LogicalType::VARCHAR, NFCNormalizeFunction);
}

} // namespace duckdb
