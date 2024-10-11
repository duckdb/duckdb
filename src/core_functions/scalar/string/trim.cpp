#include "duckdb/core_functions/scalar/string_functions.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "utf8proc.hpp"

#include <string.h>

namespace duckdb {

template <bool LTRIM, bool RTRIM>
struct TrimOperator {
	template <class INPUT_TYPE, class RESULT_TYPE>
	static RESULT_TYPE Operation(INPUT_TYPE input, Vector &result) {
		auto data = input.GetData();
		auto size = input.GetSize();

		utf8proc_int32_t codepoint;
		auto str = reinterpret_cast<const utf8proc_uint8_t *>(data);

		// Find the first character that is not left trimmed
		idx_t begin = 0;
		if (LTRIM) {
			while (begin < size) {
				auto bytes =
				    utf8proc_iterate(str + begin, UnsafeNumericCast<utf8proc_ssize_t>(size - begin), &codepoint);
				D_ASSERT(bytes > 0);
				if (utf8proc_category(codepoint) != UTF8PROC_CATEGORY_ZS) {
					break;
				}
				begin += UnsafeNumericCast<idx_t>(bytes);
			}
		}

		// Find the last character that is not right trimmed
		idx_t end;
		if (RTRIM) {
			end = begin;
			for (auto next = begin; next < size;) {
				auto bytes = utf8proc_iterate(str + next, UnsafeNumericCast<utf8proc_ssize_t>(size - next), &codepoint);
				D_ASSERT(bytes > 0);
				next += UnsafeNumericCast<idx_t>(bytes);
				if (utf8proc_category(codepoint) != UTF8PROC_CATEGORY_ZS) {
					end = next;
				}
			}
		} else {
			end = size;
		}

		// Copy the trimmed string
		auto target = StringVector::EmptyString(result, end - begin);
		auto output = target.GetDataWriteable();
		memcpy(output, data + begin, end - begin);

		target.Finalize();
		return target;
	}
};

template <bool LTRIM, bool RTRIM>
static void UnaryTrimFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	UnaryExecutor::ExecuteString<string_t, string_t, TrimOperator<LTRIM, RTRIM>>(args.data[0], result, args.size());
}

static void GetIgnoredCodepoints(string_t ignored, unordered_set<utf8proc_int32_t> &ignored_codepoints) {
	auto dataptr = reinterpret_cast<const utf8proc_uint8_t *>(ignored.GetData());
	auto size = ignored.GetSize();
	idx_t pos = 0;
	while (pos < size) {
		utf8proc_int32_t codepoint;
		pos += UnsafeNumericCast<idx_t>(
		    utf8proc_iterate(dataptr + pos, UnsafeNumericCast<utf8proc_ssize_t>(size - pos), &codepoint));
		ignored_codepoints.insert(codepoint);
	}
}

template <bool LTRIM, bool RTRIM>
static void BinaryTrimFunction(DataChunk &input, ExpressionState &state, Vector &result) {
	BinaryExecutor::Execute<string_t, string_t, string_t>(
	    input.data[0], input.data[1], result, input.size(), [&](string_t input, string_t ignored) {
		    auto data = input.GetData();
		    auto size = input.GetSize();

		    unordered_set<utf8proc_int32_t> ignored_codepoints;
		    GetIgnoredCodepoints(ignored, ignored_codepoints);

		    utf8proc_int32_t codepoint;
		    auto str = reinterpret_cast<const utf8proc_uint8_t *>(data);

		    // Find the first character that is not left trimmed
		    idx_t begin = 0;
		    if (LTRIM) {
			    while (begin < size) {
				    auto bytes =
				        utf8proc_iterate(str + begin, UnsafeNumericCast<utf8proc_ssize_t>(size - begin), &codepoint);
				    if (ignored_codepoints.find(codepoint) == ignored_codepoints.end()) {
					    break;
				    }
				    begin += UnsafeNumericCast<idx_t>(bytes);
			    }
		    }

		    // Find the last character that is not right trimmed
		    idx_t end;
		    if (RTRIM) {
			    end = begin;
			    for (auto next = begin; next < size;) {
				    auto bytes =
				        utf8proc_iterate(str + next, UnsafeNumericCast<utf8proc_ssize_t>(size - next), &codepoint);
				    D_ASSERT(bytes > 0);
				    next += UnsafeNumericCast<idx_t>(bytes);
				    if (ignored_codepoints.find(codepoint) == ignored_codepoints.end()) {
					    end = next;
				    }
			    }
		    } else {
			    end = size;
		    }

		    // Copy the trimmed string
		    auto target = StringVector::EmptyString(result, end - begin);
		    auto output = target.GetDataWriteable();
		    memcpy(output, data + begin, end - begin);

		    target.Finalize();
		    return target;
	    });
}

ScalarFunctionSet TrimFun::GetFunctions() {
	ScalarFunctionSet trim;
	trim.AddFunction(ScalarFunction({LogicalType::VARCHAR}, LogicalType::VARCHAR, UnaryTrimFunction<true, true>));

	trim.AddFunction(ScalarFunction({LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::VARCHAR,
	                                BinaryTrimFunction<true, true>));
	return trim;
}

ScalarFunctionSet LtrimFun::GetFunctions() {
	ScalarFunctionSet ltrim;
	ltrim.AddFunction(ScalarFunction({LogicalType::VARCHAR}, LogicalType::VARCHAR, UnaryTrimFunction<true, false>));
	ltrim.AddFunction(ScalarFunction({LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::VARCHAR,
	                                 BinaryTrimFunction<true, false>));
	return ltrim;
}

ScalarFunctionSet RtrimFun::GetFunctions() {
	ScalarFunctionSet rtrim;
	rtrim.AddFunction(ScalarFunction({LogicalType::VARCHAR}, LogicalType::VARCHAR, UnaryTrimFunction<false, true>));

	rtrim.AddFunction(ScalarFunction({LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::VARCHAR,
	                                 BinaryTrimFunction<false, true>));
	return rtrim;
}

} // namespace duckdb
