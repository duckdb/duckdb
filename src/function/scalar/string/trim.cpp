#include "duckdb/function/scalar/string_functions.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "utf8proc.hpp"

#include <string.h>

using namespace std;

namespace duckdb {

template <bool LTRIM, bool RTRIM>
static void unary_trim_function(DataChunk &args, ExpressionState &state, Vector &result) {
	UnaryExecutor::Execute<string_t, string_t, true>(args.data[0], result, args.size(), [&](string_t input) {
		const auto data = input.GetData();
		const auto size = input.GetSize();

		utf8proc_int32_t codepoint;
		const auto str = reinterpret_cast<const utf8proc_uint8_t *>(data);

		// Find the first character that is not left trimmed
		idx_t begin = 0;
		if (LTRIM) {
			while (begin < size) {
				const auto bytes = utf8proc_iterate(str + begin, size - begin, &codepoint);
				assert(bytes > 0);
				if (utf8proc_category(codepoint) != UTF8PROC_CATEGORY_ZS) {
					break;
				}
				begin += bytes;
			}
		}

		// Find the last character that is not right trimmed
		idx_t end;
		if (RTRIM) {
			end = begin;
			for (auto next = begin; next < size;) {
				const auto bytes = utf8proc_iterate(str + next, size - next, &codepoint);
				assert(bytes > 0);
				next += bytes;
				if (utf8proc_category(codepoint) != UTF8PROC_CATEGORY_ZS) {
					end = next;
				}
			}
		} else {
			end = size;
		}

		// Copy the trimmed string
		auto target = StringVector::EmptyString(result, end - begin);
		auto output = target.GetData();
		memcpy(output, data + begin, end - begin);

		target.Finalize();
		return target;
	});
}

static void get_ignored_codepoints(string_t ignored, unordered_set<utf8proc_int32_t> &ignored_codepoints) {
	const auto dataptr = (utf8proc_uint8_t*) ignored.GetData();
	const auto size = ignored.GetSize();
	idx_t pos = 0;
	while(pos < size) {
		utf8proc_int32_t codepoint;
		pos += utf8proc_iterate(dataptr + pos, size - pos, &codepoint);
		ignored_codepoints.insert(codepoint);
	}
}

template <bool LTRIM, bool RTRIM> static void binary_trim_function(DataChunk &input, ExpressionState &state, Vector &result) {
	BinaryExecutor::Execute<string_t, string_t, string_t, true>(input.data[0], input.data[1], result, input.size(), [&](string_t input, string_t ignored) {
		const auto data = input.GetData();
		const auto size = input.GetSize();

		unordered_set<utf8proc_int32_t> ignored_codepoints;
		get_ignored_codepoints(ignored, ignored_codepoints);

		utf8proc_int32_t codepoint;
		const auto str = reinterpret_cast<const utf8proc_uint8_t *>(data);

		// Find the first character that is not left trimmed
		idx_t begin = 0;
		if (LTRIM) {
			while (begin < size) {
				const auto bytes = utf8proc_iterate(str + begin, size - begin, &codepoint);
				if (ignored_codepoints.find(codepoint) == ignored_codepoints.end()) {
					break;
				}
				begin += bytes;
			}
		}

		// Find the last character that is not right trimmed
		idx_t end;
		if (RTRIM) {
			end = begin;
			for (auto next = begin; next < size;) {
				const auto bytes = utf8proc_iterate(str + next, size - next, &codepoint);
				assert(bytes > 0);
				next += bytes;
				if (ignored_codepoints.find(codepoint) == ignored_codepoints.end()) {
					end = next;
				}
			}
		} else {
			end = size;
		}

		// Copy the trimmed string
		auto target = StringVector::EmptyString(result, end - begin);
		auto output = target.GetData();
		memcpy(output, data + begin, end - begin);

		target.Finalize();
		return target;
	});
}

void TrimFun::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunctionSet ltrim("ltrim");
	ScalarFunctionSet rtrim("rtrim");
	ScalarFunctionSet trim("trim");

	ltrim.AddFunction(ScalarFunction({LogicalType::VARCHAR}, LogicalType::VARCHAR, unary_trim_function<true, false>));
	rtrim.AddFunction(ScalarFunction({LogicalType::VARCHAR}, LogicalType::VARCHAR, unary_trim_function<false, true>));
	trim.AddFunction(ScalarFunction({LogicalType::VARCHAR}, LogicalType::VARCHAR, unary_trim_function<true, true>));

	ltrim.AddFunction(ScalarFunction({LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::VARCHAR, binary_trim_function<true, false>));
	rtrim.AddFunction(ScalarFunction({LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::VARCHAR, binary_trim_function<false, true>));
	trim.AddFunction(ScalarFunction({LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::VARCHAR, binary_trim_function<true, true>));

	set.AddFunction(ltrim);
	set.AddFunction(rtrim);
	set.AddFunction(trim);
}

} // namespace duckdb
