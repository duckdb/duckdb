#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/function/scalar/string_functions.hpp"
#include "duckdb/function/scalar/regexp.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/types/blob.hpp"
#include "duckdb/main/extension_util.hpp"
#include "utf8proc_wrapper.hpp"
#include "inet_extension.hpp"
#include "inet_functions.hpp"
#include "html_charref.hpp"
#include "re2/re2.h"

namespace duckdb {

struct RegexpUnescapeBindData : public RegexpMatchesBindData {
	const unordered_map<string, uint32_t> html5_names;
	const unordered_set<uint32_t> invalid_codepoints;
	const unordered_map<uint32_t, string> invalid_charrefs;

	RegexpUnescapeBindData(duckdb_re2::RE2::Options options)
	    : RegexpMatchesBindData(options, std::move("&(#[0-9]+;?|#[xX][0-9a-fA-F]+;?|[^\t\n\f <&#;]{1,32};?)"), true),
	      html5_names(ReturnHTML5NameCharrefs()), invalid_codepoints(ReturnInvalidCodepoints()),
	      invalid_charrefs(ReturnInvalidCharrefs()) {
	}
};

unique_ptr<FunctionData> UnescapeBind(ClientContext &context, ScalarFunction &bound_function,
                                      vector<unique_ptr<Expression>> &arguments) {
	duckdb_re2::RE2::Options options;
	options.set_log_errors(false); // disable error logging for performance
	options.set_case_sensitive(false);
	return make_uniq<RegexpUnescapeBindData>(options);
}

static void PrepareStrForTypeCasting(string &match) {
	// in HTML, the sequence &#<number>; represents a character by its Unicode code point or in the form
	// &#x<hex_number>; which represents its hexadecimal value.
	// To apply the TryCast::Operation, this string requires some "cleaning"
	match.erase(match.begin()); // rmv # char
	if (match.back() == ';') {  // rmv ; char
		match.pop_back();
	}

	// TryCast::Operation needs the hexadecimal values to start with 0x
	if (match[0] == 'x' || match[0] == 'X') {
		match.insert(0, "0");
	}
}

static bool AddBlob(uint32_t code_point, string &result) {
	int sz = 0;
	char c[4] = {'\0', '\0', '\0', '\0'};
	if (!Utf8Proc::CodepointToUtf8(code_point, sz, c)) {
		return false;
	}
	result += c;
	return true;
}

static string ReplaceCharref(string_t &str, RegexpUnescapeBindData &info, RE2 &pattern) {
	string match;
	string result {""};
	idx_t start_pos = 0;
	auto input_data = str.GetData();
	auto input_size = str.GetSize();
	auto input = regexp_util::CreateStringPiece(str); // wrap a StringPiece around it
	while (RE2::FindAndConsume(&input, pattern, &match)) {
		// include the input value until the current match
		idx_t match_pos = input_size - (match.length() + 1) - input.length(); // +1 to include also the & ampersand char
		result += string(input_data + start_pos, match_pos - start_pos);

		// the position after the end of the current match
		start_pos = (match_pos + 1) + match.length();

		if (match[0] == '#') { // true, if it represents a hexadecimal value or a Unicode point
			int32_t num;
			PrepareStrForTypeCasting(match);
			if (!TryCast::Operation<string_t, int32_t>(string_t(match), num)) {
				result += "\uFFFD";
				continue;
			}

			if (info.invalid_charrefs.find(num) != info.invalid_charrefs.end()) {
				auto ch = info.invalid_charrefs.at(num);
				if (Utf8Proc::Analyze(ch.c_str(), ch.length()) == UnicodeType::INVALID) {
					auto blob = string_t(info.invalid_charrefs.at(num));
					auto str_blob = Blob::ToString(blob);
					result += str_blob;
				} else {
					result += ch;
				}

			} else if ((0xD800 <= num && num <= 0xDFFF) || 0x10FFFF < num) {
				result += "\uFFFD";
			} else if (info.invalid_codepoints.find(num) != info.invalid_codepoints.end()) {
				// skip this character
				continue;
			} else {
				result += static_cast<char>(num);
			}
		} else { // named character reference
			auto it = info.html5_names.find(match);
			if (it != info.html5_names.end()) {
				if (!AddBlob(it->second, result)) {
					throw ConversionException("Cannot convert codepoint of %s", match);
				}
				continue;
			}
			for (idx_t x = match.length() - 1; x >= 1; --x) {
				string substr = match.substr(0, x);
				it = info.html5_names.find(substr);
				if (it != info.html5_names.end()) {
					if (!AddBlob(it->second, result)) {
						throw ConversionException("Cannot convert codepoint of %s to utf8", match);
					}
					start_pos -= (match.length() - x);
					input.set(input_data + start_pos, input_size - start_pos);
					break;
				}
			}
			if (it == info.html5_names.end()) {
				result.append('&' + match);
			}
		}
	}
	input.AppendToString(&result);
	return result;
}

static void EscapeInputStr(string_t &input_str, string &escaped, bool input_quote) {
	const auto str = input_str.GetData();
	const auto str_size = input_str.GetSize();
	for (idx_t i = 0; i < str_size; ++i) {
		char ch = str[i];
		switch (ch) {
		case '&':
			escaped += "&amp;";
			break;
		case '<':
			escaped += "&lt;";
			break;
		case '>':
			escaped += "&gt;";
			break;
		case '"':
			if (input_quote) {
				escaped += "&quot;";
			} else {
				escaped += "\"";
			}
			break;
		case '\'':
			if (input_quote) {
				escaped += "&#x27;";
			} else {
				escaped += "\'";
			}
			break;
		default:
			escaped += ch;
		}
	}
}

void INetFunctions::Escape(DataChunk &args, ExpressionState &state, Vector &result) {
	auto escape_string = [&](string_t &input_str, bool input_quote) {
		string escaped {""};
		EscapeInputStr(input_str, escaped, input_quote);
		return StringVector::AddString(result, escaped);
	};
	if (args.ColumnCount() == 1) {
		UnaryExecutor::Execute<string_t, string_t>(args.data[0], result, args.size(),
		                                           [&](string_t input_str) { return escape_string(input_str, true); });
	} else {
		BinaryExecutor::Execute<string_t, bool, string_t>(
		    args.data[0], args.data[1], result, args.size(),
		    [&](string_t input_str, bool input_quote) { return escape_string(input_str, input_quote); });
	}
}

void INetFunctions::Unescape(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	auto &info = func_expr.bind_info->Cast<RegexpUnescapeBindData>();
	auto &lstate = ExecuteFunctionState::GetFunctionState(state)->Cast<RegexLocalState>();
	UnaryExecutor::Execute<string_t, string_t>(args.data[0], result, args.size(), [&](string_t input_st) {
		// check whether the input contains an ampersand character. If not, return the input value unchanged.
		if (ContainsFun::Find(const_uchar_ptr_cast(input_st.GetData()), input_st.GetSize(), const_uchar_ptr_cast("&"), 1) == DConstants::INVALID_INDEX) {
			return StringVector::AddString(result, input_st);
		}
		string unescaped = ReplaceCharref(input_st, info, lstate.constant_pattern);
		return StringVector::AddString(result, unescaped);
	});
}

ScalarFunctionSet InetExtension::GetEscapeFunctionSet() {
	ScalarFunctionSet funcs("html_escape");
	funcs.AddFunction(ScalarFunction({LogicalType::VARCHAR}, LogicalType::VARCHAR, INetFunctions::Escape, nullptr,
	                                 nullptr, nullptr, nullptr, LogicalType::INVALID, FunctionStability::CONSISTENT,
	                                 FunctionNullHandling::DEFAULT_NULL_HANDLING));
	funcs.AddFunction(ScalarFunction({LogicalType::VARCHAR, LogicalType::BOOLEAN}, LogicalType::VARCHAR,
	                                 INetFunctions::Escape, nullptr, nullptr, nullptr, nullptr, LogicalType::INVALID,
	                                 FunctionStability::CONSISTENT, FunctionNullHandling::DEFAULT_NULL_HANDLING));
	return funcs;
}

ScalarFunction InetExtension::GetUnescapeFunction() {
	return ScalarFunction("html_unescape", {LogicalType::VARCHAR}, LogicalType::VARCHAR, INetFunctions::Unescape,
	                      UnescapeBind, nullptr, nullptr, RegexInitLocalState, LogicalType::INVALID,
	                      FunctionStability::CONSISTENT, FunctionNullHandling::DEFAULT_NULL_HANDLING);
}

} // namespace duckdb
