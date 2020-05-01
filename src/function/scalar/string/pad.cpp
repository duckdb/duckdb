#include "duckdb/function/scalar/string_functions.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/vector_operations/ternary_executor.hpp"
#include "utf8proc.hpp"

using namespace std;

namespace duckdb {

static pair<idx_t, idx_t> count_chars(const idx_t len, const char *data, const idx_t size) {
	//  Count how much of str will fit in the output
	auto str = reinterpret_cast<const utf8proc_uint8_t *>(data);
	idx_t nbytes = 0;
	idx_t nchars = 0;
	for (; nchars < len && nbytes < size; ++nchars) {
		utf8proc_int32_t codepoint;
		const auto bytes = utf8proc_iterate(str + nbytes, size - nbytes, &codepoint);
		assert(bytes > 0);
		nbytes += bytes;
	}

	return pair<idx_t, idx_t>(nbytes, nchars);
}

static bool insert_padding(const idx_t len, const string_t &pad, vector<char> &result) {
	//  Copy the padding until the output is long enough
	const auto data = pad.GetData();
	const auto size = pad.GetSize();

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
		const auto bytes = utf8proc_iterate(str + nbytes, size - nbytes, &codepoint);
		assert(bytes > 0);
		nbytes += bytes;
	}

	//  Flush the remaining pad
	result.insert(result.end(), data, data + nbytes);

	return true;
}

static string_t lpad(const string_t &str, const int32_t len, const string_t &pad, vector<char> &result) {
	//  Reuse the buffer
	result.clear();

	// Get information about the base string
	const auto data_str = str.GetData();
	const auto size_str = str.GetSize();

	//  Count how much of str will fit in the output
	const auto written = count_chars(len, data_str, size_str);

	//  Left pad by the number of characters still needed
	if (!insert_padding(len - written.second, pad, result)) {
		throw Exception("Insufficient padding in LPAD.");
	}

	//  Append as much of the original string as fits
	result.insert(result.end(), data_str, data_str + written.first);

	return string_t(result.data(), result.size());
}

struct LpadOperator {
	static inline string_t Operation(const string_t &str, const int32_t len, const string_t &pad,
	                                 vector<char> &result) {
		return lpad(str, len, pad, result);
	}
};

static string_t rpad(const string_t &str, const int32_t len, const string_t &pad, vector<char> &result) {
	//  Reuse the buffer
	result.clear();

	// Get information about the base string
	const auto data_str = str.GetData();
	const auto size_str = str.GetSize();

	//  Count how much of str will fit in the output
	const auto written = count_chars(len, data_str, size_str);

	//  Append as much of the original string as fits
	result.insert(result.end(), data_str, data_str + written.first);

	//  Right pad by the number of characters still needed
	if (!insert_padding(len - written.second, pad, result)) {
		throw Exception("Insufficient padding in RPAD.");
	};

	return string_t(result.data(), result.size());
}

struct RpadOperator {
	static inline string_t Operation(const string_t &str, const int32_t len, const string_t &pad,
	                                 vector<char> &result) {
		return rpad(str, len, pad, result);
	}
};

template <class Op> static void pad_function(DataChunk &args, ExpressionState &state, Vector &result) {
	assert(args.column_count() == 3 && args.data[0].type == TypeId::VARCHAR && args.data[1].type == TypeId::INT32 &&
	       args.data[2].type == TypeId::VARCHAR);
	auto &str_vector = args.data[0];
	auto &len_vector = args.data[1];
	auto &pad_vector = args.data[2];

	vector<char> buffer;
	TernaryExecutor::Execute<string_t, int32_t, string_t, string_t>(
	    str_vector, len_vector, pad_vector, result, args.size(), [&](string_t str, int32_t len, string_t pad) {
		    len = max(len, int32_t(0));
		    return StringVector::AddString(result, Op::Operation(str, len, pad, buffer));
	    });
}

void LpadFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(ScalarFunction("lpad",                              // name of the function
	                               {SQLType::VARCHAR, SQLType::INTEGER, // argument list
	                                SQLType::VARCHAR},
	                               SQLType::VARCHAR,             // return type
	                               pad_function<LpadOperator>)); // pointer to function implementation
}

void RpadFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(ScalarFunction("rpad",                              // name of the function
	                               {SQLType::VARCHAR, SQLType::INTEGER, // argument list
	                                SQLType::VARCHAR},
	                               SQLType::VARCHAR,             // return type
	                               pad_function<RpadOperator>)); // pointer to function implementation
}

} // namespace duckdb
