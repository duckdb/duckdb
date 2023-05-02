#include "duckdb/core_functions/scalar/string_functions.hpp"
#include "utf8proc.hpp"
#include "utf8proc_wrapper.hpp"

namespace duckdb {

struct ChrOperator {
	static void GetCodepoint(int32_t input, char c[], int &utf8_bytes) {
		if (input < 0 || !Utf8Proc::CodepointToUtf8(input, utf8_bytes, &c[0])) {
			throw InvalidInputException("Invalid UTF8 Codepoint %d", input);
		}
	}

	template <class TA, class TR>
	static inline TR Operation(const TA &input) {
		char c[5] = {'\0', '\0', '\0', '\0', '\0'};
		int utf8_bytes;
		GetCodepoint(input, c, utf8_bytes);
		return string_t(&c[0], utf8_bytes);
	}
};

#ifdef DUCKDB_DEBUG_NO_INLINE
// the chr function depends on the data always being inlined (which is always possible, since it outputs max 4 bytes)
// to enable chr when string inlining is disabled we create a special function here
static void ChrFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &code_vec = args.data[0];

	char c[5] = {'\0', '\0', '\0', '\0', '\0'};
	int utf8_bytes;
	UnaryExecutor::Execute<int32_t, string_t>(code_vec, result, args.size(), [&](int32_t input) {
		ChrOperator::GetCodepoint(input, c, utf8_bytes);
		return StringVector::AddString(result, &c[0], utf8_bytes);
	});
}
#endif

ScalarFunction ChrFun::GetFunction() {
	return ScalarFunction("chr", {LogicalType::INTEGER}, LogicalType::VARCHAR,
#ifdef DUCKDB_DEBUG_NO_INLINE
	                      ChrFunction
#else
	                      ScalarFunction::UnaryFunction<int32_t, string_t, ChrOperator>
#endif
	);
}

} // namespace duckdb
