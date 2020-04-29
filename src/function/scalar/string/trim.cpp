#include "duckdb/function/scalar/string_functions.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "utf8proc.hpp"

#include <string.h>

using namespace std;

namespace duckdb {

struct SpaceChar {
	static char Operation(utf8proc_int32_t codepoint) {
		return UTF8PROC_CATEGORY_ZS == utf8proc_category(codepoint);
	}
};

struct KeptChar {
	static char Operation(utf8proc_int32_t codepoint) {
		return false;
	}
};

template <class LTRIM, class RTRIM> static void trim_function(Vector &input, Vector &result, idx_t count) {
	assert(input.type == TypeId::VARCHAR);

	UnaryExecutor::Execute<string_t, string_t, true>(input, result, count, [&](string_t input) {
		const auto data = input.GetData();
		const auto size = input.GetSize();

		utf8proc_int32_t codepoint;
		const auto str = reinterpret_cast<const utf8proc_uint8_t *>(data);

		//  Find the first character that is not left trimmed
		idx_t begin = 0;
		while (begin < size) {
			const auto bytes = utf8proc_iterate(str + begin, size - begin, &codepoint);
			assert(bytes > 0);
			if (!LTRIM::Operation(codepoint)) {
				break;
			}
			begin += bytes;
		}

		//  Find the last character that is not right trimmed
		idx_t end = size;
		for (auto next = begin; next < size;) {
			const auto bytes = utf8proc_iterate(str + next, size - next, &codepoint);
			assert(bytes > 0);
			next += bytes;
			if (!RTRIM::Operation(codepoint)) {
				end = next;
			}
		}

		//  Copy the trimmed string
		auto target = StringVector::EmptyString(result, end - begin);
		auto output = target.GetData();
		memcpy(output, data + begin, end - begin);

		target.Finalize();
		return target;
	});
}

static void trim_ltrim_function(DataChunk &args, ExpressionState &state, Vector &result) {
	assert(args.column_count() == 1);
	trim_function<SpaceChar, KeptChar>(args.data[0], result, args.size());
}

static void trim_rtrim_function(DataChunk &args, ExpressionState &state, Vector &result) {
	assert(args.column_count() == 1);
	trim_function<KeptChar, SpaceChar>(args.data[0], result, args.size());
}

void LtrimFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(ScalarFunction("ltrim", {SQLType::VARCHAR}, SQLType::VARCHAR, trim_ltrim_function));
}

void RtrimFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(ScalarFunction("rtrim", {SQLType::VARCHAR}, SQLType::VARCHAR, trim_rtrim_function));
}

} // namespace duckdb
