#include "duckdb/core_functions/scalar/string_functions.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

static string_t escape(const string_t &str, vector<char> &escaped_pattern) {
	auto input_str = str.GetData();
	auto size_str = str.GetSize();

	escaped_pattern.clear(); // reuse the buffer
	// note: reserving double the size to account for escaping ('\\') as an average case
	// to have half of the characters in the input string are special
	escaped_pattern.reserve(2 * size_str);
	string special_chars = "()[]{}?*+-|^$\\.&~#";
	for (idx_t i = 0; i < size_str; ++i) {
		char ch = input_str[i];
		if (special_chars.find(ch) != std::string::npos) {
			escaped_pattern.push_back('\\'); // escape the special character
		}
		escaped_pattern.push_back(ch);
	}
	return string_t(escaped_pattern.data(), escaped_pattern.size());
}

static void RegexpEscapeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	vector<char> escaped_pattern;
	auto input_str = args.GetValue(0, 0);
	result.Reference(escape(input_str.ToString(), escaped_pattern));
}

ScalarFunction RegexpEscapeFun::GetFunction() {
	return ScalarFunction({LogicalType::VARCHAR}, LogicalType::VARCHAR, RegexpEscapeFunction);
}

} // namespace duckdb
