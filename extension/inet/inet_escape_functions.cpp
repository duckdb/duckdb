#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/function/scalar/string_functions.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/common/types/blob.hpp"
#include "utf8proc_wrapper.hpp"
#include "inet_extension.hpp"
#include "inet_functions.hpp"
#include "html_charref.hpp"
#include "re2/re2.h"

namespace duckdb {

// see https://html.spec.whatwg.org/multipage/parsing.html#numeric-character-reference-end-state
const unordered_map<uint32_t, string> ReturnInvalidCharrefs() {
	return {
	    {0x00, "\ufffd"}, // REPLACEMENT CHARACTER
	    {0x0d, "\r"},     // CARRIAGE RETURN
	    {0x80, "\u20ac"}, // EURO SIGN
	    {0x81, "\x81"},   // control code point
	    {0x82, "\u201a"}, // SINGLE LOW-9 QUOTATION MARK
	    {0x83, "\u0192"}, // LATIN SMALL LETTER F WITH HOOK
	    {0x84, "\u201e"}, // DOUBLE LOW-9 QUOTATION MARK
	    {0x85, "\u2026"}, // HORIZONTAL ELLIPSIS
	    {0x86, "\u2020"}, // DAGGER
	    {0x87, "\u2021"}, // DOUBLE DAGGER
	    {0x88, "\u02c6"}, // MODIFIER LETTER CIRCUMFLEX ACCENT
	    {0x89, "\u2030"}, // PER MILLE SIGN
	    {0x8a, "\u0160"}, // LATIN CAPITAL LETTER S WITH CARON
	    {0x8b, "\u2039"}, // SINGLE LEFT-POINTING ANGLE QUOTATION MARK
	    {0x8c, "\u0152"}, // LATIN CAPITAL LIGATURE OE
	    {0x8d, "\x8d"},   // control code point
	    {0x8e, "\u017d"}, // LATIN CAPITAL LETTER Z WITH CARON
	    {0x8f, "\x8f"},   // control code point
	    {0x90, "\x90"},   // control code point
	    {0x91, "\u2018"}, // LEFT SINGLE QUOTATION MARK
	    {0x92, "\u2019"}, // RIGHT SINGLE QUOTATION MARK
	    {0x93, "\u201c"}, // LEFT DOUBLE QUOTATION MARK
	    {0x94, "\u201d"}, // RIGHT DOUBLE QUOTATION MARK
	    {0x95, "\u2022"}, // BULLET
	    {0x96, "\u2013"}, // EN DASH
	    {0x97, "\u2014"}, // EM DASH
	    {0x98, "\u02dc"}, // SMALL TILDE
	    {0x99, "\u2122"}, // TRADE MARK SIGN
	    {0x9a, "\u0161"}, // LATIN SMALL LETTER S WITH CARON
	    {0x9b, "\u203a"}, // SINGLE RIGHT-POINTING ANGLE QUOTATION MARK
	    {0x9c, "\u0153"}, // LATIN SMALL LIGATURE OE
	    {0x9d, "\x9d"},   // control code point
	    {0x9e, "\u017e"}, // LATIN SMALL LETTER Z WITH CARON
	    {0x9f, "\u0178"}  // LATIN CAPITAL LETTER Y WITH DIAERESIS
	};
}

const unordered_set<uint32_t> ReturnInvalidCodepoints() {
	return {// 0x0001 to 0x0008
	        0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x8,
	        // 0x000E to 0x001F
	        0xe, 0xf, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f,
	        // 0x007F to 0x009F
	        0x7f, 0x80, 0x81, 0x82, 0x83, 0x84, 0x85, 0x86, 0x87, 0x88, 0x89, 0x8a, 0x8b, 0x8c, 0x8d, 0x8e, 0x8f, 0x90,
	        0x91, 0x92, 0x93, 0x94, 0x95, 0x96, 0x97, 0x98, 0x99, 0x9a, 0x9b, 0x9c, 0x9d, 0x9e, 0x9f,
	        // 0xFDD0 to 0xFDEF
	        0xfdd0, 0xfdd1, 0xfdd2, 0xfdd3, 0xfdd4, 0xfdd5, 0xfdd6, 0xfdd7, 0xfdd8, 0xfdd9, 0xfdda, 0xfddb, 0xfddc,
	        0xfddd, 0xfdde, 0xfddf, 0xfde0, 0xfde1, 0xfde2, 0xfde3, 0xfde4, 0xfde5, 0xfde6, 0xfde7, 0xfde8, 0xfde9,
	        0xfdea, 0xfdeb, 0xfdec, 0xfded, 0xfdee, 0xfdef,
	        // others
	        0xb, 0xfffe, 0xffff, 0x1fffe, 0x1ffff, 0x2fffe, 0x2ffff, 0x3fffe, 0x3ffff, 0x4fffe, 0x4ffff, 0x5fffe,
	        0x5ffff, 0x6fffe, 0x6ffff, 0x7fffe, 0x7ffff, 0x8fffe, 0x8ffff, 0x9fffe, 0x9ffff, 0xafffe, 0xaffff, 0xbfffe,
	        0xbffff, 0xcfffe, 0xcffff, 0xdfffe, 0xdffff, 0xefffe, 0xeffff, 0xffffe, 0xfffff, 0x10fffe, 0x10ffff};
}

struct UnescapeBindData : public FunctionData {
	unique_ptr<RE2> charref;
	const unordered_map<string, uint32_t> html5_names;
	const unordered_set<uint32_t> invalid_codepoints;
	const unordered_map<uint32_t, string> invalid_charrefs;

	UnescapeBindData()
	    : html5_names(ReturnHTML5NameCharrefs()), invalid_codepoints(ReturnInvalidCodepoints()),
	      invalid_charrefs(ReturnInvalidCharrefs()) {
		RE2::Options options;
		options.set_log_errors(false); // disable error logging for performance
		options.set_case_sensitive(false);
		charref = make_uniq<RE2>(R"(&(#[0-9]+;?|#[xX][0-9a-fA-F]+;?|[^\t\n\f <&#;]{1,32};?))", options);
	}

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<UnescapeBindData>();
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<UnescapeBindData>();
		bool equal_sizes = (html5_names.size() == other.html5_names.size() &&
		                    invalid_codepoints.size() == other.invalid_codepoints.size() &&
		                    invalid_charrefs.size() == other.invalid_charrefs.size());
		return (equal_sizes && (html5_names == other.html5_names && invalid_codepoints == other.invalid_codepoints &&
		                        invalid_charrefs == other.invalid_charrefs));
	}
};

unique_ptr<FunctionData> UnescapeBind(ClientContext &context, ScalarFunction &bound_function,
                                      vector<unique_ptr<Expression>> &arguments) {
	return make_uniq<UnescapeBindData>();
}

static void AddBlob(uint32_t code_point, string &result) {
	int sz = 0;
	char c[4] = {'\0', '\0', '\0', '\0'};
	D_ASSERT(Utf8Proc::CodepointToUtf8(code_point, sz, c));
	result += c;
}

static void PrepareStrForTypeCasting(string &match) {
	// in HTML, the sequence &# followed by a number and a semicolon(;)
	// represents a character by its Unicode code point or its hexadecimal value.
	// To apply the TryCast::Operation, this string requires "cleaning"
	match.erase(match.begin()); // rmv # char
	if (match.back() == ';') { // rmv ; char
		match.pop_back();
	}
	// TryCast::Operation needs the hexadecimal values to start with 0x
	if (match[0] == 'x' || match[0] == 'X') {
		match.insert(0, "0");
	}
}

static string ReplaceCharref(string &str, UnescapeBindData &info) {
	string match;
	string result {""};
	idx_t start_pos = 0;
	duckdb_re2::StringPiece input(str); // wrap a StringPiece around it
	while (RE2::FindAndConsume(&input, *info.charref, &match)) {
		// include the input value until the match
		idx_t match_pos = str.find(match, start_pos) - 1; // include also the & ampersand char
		result += str.substr(start_pos, match_pos - start_pos);

		// the position after the end of the current match
		start_pos = (match_pos + 1) + match.length();

		if (match[0] == '#') {
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
		} else {
			// named charref
			auto it = info.html5_names.find(match);
			if (it != info.html5_names.end()) {
				AddBlob(it->second, result);
				continue;
			}

			for (idx_t x = match.length() - 1; x >= 1; --x) {
				string substr = match.substr(0, x);
				it = info.html5_names.find(substr);
				if (it != info.html5_names.end()) {
					AddBlob(it->second, result);
					start_pos = x + 1;
					break;
				}
			}
			if (it == info.html5_names.end()) {
				result.append('&' + match);
			}
		}
	}
	result += str.substr(start_pos);
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
	auto &info = func_expr.bind_info->Cast<UnescapeBindData>();
	UnaryExecutor::Execute<string_t, string_t>(args.data[0], result, args.size(), [&](string_t input_st) {
		auto str = input_st.GetString();
		if (str.find('&') == str.npos) {
			return StringVector::AddString(result, str);
		}
		string unescaped = ReplaceCharref(str, info);
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
	                      UnescapeBind, nullptr, nullptr, nullptr, LogicalType::INVALID, FunctionStability::CONSISTENT,
	                      FunctionNullHandling::DEFAULT_NULL_HANDLING);
}

} // namespace duckdb
