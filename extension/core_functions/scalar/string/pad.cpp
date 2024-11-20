#include "core_functions/scalar/string_functions.hpp"

#include "duckdb/common/algorithm.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/vector_operations/ternary_executor.hpp"
#include "duckdb/common/pair.hpp"

#include "utf8proc.hpp"

namespace duckdb {

static pair<idx_t, idx_t> PadCountChars(const idx_t len, const char *data, const idx_t size) {
	//  Count how much of str will fit in the output
	auto str = reinterpret_cast<const utf8proc_uint8_t *>(data);
	idx_t nbytes = 0;
	idx_t nchars = 0;
	for (; nchars < len && nbytes < size; ++nchars) {
		utf8proc_int32_t codepoint;
		auto bytes = utf8proc_iterate(str + nbytes, UnsafeNumericCast<utf8proc_ssize_t>(size - nbytes), &codepoint);
		D_ASSERT(bytes > 0);
		nbytes += UnsafeNumericCast<idx_t>(bytes);
	}

	return pair<idx_t, idx_t>(nbytes, nchars);
}

static bool InsertPadding(const idx_t len, const string_t &pad, vector<char> &result) {
	//  Copy the padding until the output is long enough
	auto data = pad.GetData();
	auto size = pad.GetSize();

	//  Check whether we need data that we don't have
	if (len > 0 && size == 0) {
		return false;
	}

	//  Insert characters until we have all we need.
	auto str = reinterpret_cast<const utf8proc_uint8_t *>(data);
	idx_t nbytes = 0;
	for (idx_t nchars = 0; nchars < len; ++nchars) {
		//  If we are at the end of the pad, flush all of it and loop back
		if (nbytes >= size) {
			result.insert(result.end(), data, data + size);
			nbytes = 0;
		}

		//  Write the next character
		utf8proc_int32_t codepoint;
		auto bytes = utf8proc_iterate(str + nbytes, UnsafeNumericCast<utf8proc_ssize_t>(size - nbytes), &codepoint);
		D_ASSERT(bytes > 0);
		nbytes += UnsafeNumericCast<idx_t>(bytes);
	}

	//  Flush the remaining pad
	result.insert(result.end(), data, data + nbytes);

	return true;
}

static string_t LeftPadFunction(const string_t &str, const int32_t len, const string_t &pad, vector<char> &result) {
	//  Reuse the buffer
	result.clear();

	// Get information about the base string
	auto data_str = str.GetData();
	auto size_str = str.GetSize();

	//  Count how much of str will fit in the output
	auto written = PadCountChars(UnsafeNumericCast<idx_t>(len), data_str, size_str);

	//  Left pad by the number of characters still needed
	if (!InsertPadding(UnsafeNumericCast<idx_t>(len) - written.second, pad, result)) {
		throw InvalidInputException("Insufficient padding in LPAD.");
	}

	//  Append as much of the original string as fits
	result.insert(result.end(), data_str, data_str + written.first);

	return string_t(result.data(), UnsafeNumericCast<uint32_t>(result.size()));
}

struct LeftPadOperator {
	static inline string_t Operation(const string_t &str, const int32_t len, const string_t &pad,
	                                 vector<char> &result) {
		return LeftPadFunction(str, len, pad, result);
	}
};

static string_t RightPadFunction(const string_t &str, const int32_t len, const string_t &pad, vector<char> &result) {
	//  Reuse the buffer
	result.clear();

	// Get information about the base string
	auto data_str = str.GetData();
	auto size_str = str.GetSize();

	// Count how much of str will fit in the output
	auto written = PadCountChars(UnsafeNumericCast<idx_t>(len), data_str, size_str);

	//  Append as much of the original string as fits
	result.insert(result.end(), data_str, data_str + written.first);

	//  Right pad by the number of characters still needed
	if (!InsertPadding(UnsafeNumericCast<idx_t>(len) - written.second, pad, result)) {
		throw InvalidInputException("Insufficient padding in RPAD.");
	};

	return string_t(result.data(), UnsafeNumericCast<uint32_t>(result.size()));
}

struct RightPadOperator {
	static inline string_t Operation(const string_t &str, const int32_t len, const string_t &pad,
	                                 vector<char> &result) {
		return RightPadFunction(str, len, pad, result);
	}
};

template <class OP>
static void PadFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &str_vector = args.data[0];
	auto &len_vector = args.data[1];
	auto &pad_vector = args.data[2];

	vector<char> buffer;
	TernaryExecutor::Execute<string_t, int32_t, string_t, string_t>(
	    str_vector, len_vector, pad_vector, result, args.size(), [&](string_t str, int32_t len, string_t pad) {
		    len = MaxValue<int32_t>(len, 0);
		    return StringVector::AddString(result, OP::Operation(str, len, pad, buffer));
	    });
}

ScalarFunction LpadFun::GetFunction() {
	return ScalarFunction({LogicalType::VARCHAR, LogicalType::INTEGER, LogicalType::VARCHAR}, LogicalType::VARCHAR,
	                      PadFunction<LeftPadOperator>);
}

ScalarFunction RpadFun::GetFunction() {
	return ScalarFunction({LogicalType::VARCHAR, LogicalType::INTEGER, LogicalType::VARCHAR}, LogicalType::VARCHAR,
	                      PadFunction<RightPadOperator>);
}

} // namespace duckdb
