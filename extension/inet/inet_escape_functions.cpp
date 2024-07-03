#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/common/types/blob.hpp"
#include "utf8proc_wrapper.hpp"
#include "inet_extension.hpp"
#include "inet_functions.hpp"
#include "html_charref.hpp"

namespace duckdb {

struct UnescapeBindData : public FunctionData {
	const HTML5NameCharrefs html5_names;
	const unordered_set<uint32_t> invalid_codepoints;
	const unordered_map<uint32_t, const char *> special_invalid_charrefs;

	UnescapeBindData()
	    : html5_names(HTML5NameCharrefs()), invalid_codepoints(ReturnInvalidCodepoints()),
	      special_invalid_charrefs(ReturnInvalidCharrefs()) {
	}

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<UnescapeBindData>();
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<UnescapeBindData>();
		bool equal_sizes = (html5_names.mapped_strings.size() == other.html5_names.mapped_strings.size() &&
		                    invalid_codepoints.size() == other.invalid_codepoints.size() &&
		                    special_invalid_charrefs.size() == other.special_invalid_charrefs.size());
		return (equal_sizes && (html5_names.mapped_strings == other.html5_names.mapped_strings &&
		                        invalid_codepoints == other.invalid_codepoints &&
		                        special_invalid_charrefs == other.special_invalid_charrefs));
	}
};

unique_ptr<FunctionData> UnescapeBind(ClientContext &context, ScalarFunction &bound_function,
                                      vector<unique_ptr<Expression>> &arguments) {
	return make_uniq<UnescapeBindData>();
}

enum class MatchType {
	Unknown, // 0
	CharRef, // 1 &<charref>
	Decimal, // 2 &#<dec_val>
	Hex      // 3 &#x<hex_val>
};

struct MatchLengthCounter {
	using RESULT_TYPE = idx_t;

	static void Operation(const char *input_data, idx_t start, idx_t count, RESULT_TYPE &result) {
		result += count;
	}
	static void Operation(const char *input_data, RESULT_TYPE &result) {
		Operation(input_data, 0, strlen(input_data), result);
	}
};

struct MatchWriter {
	using RESULT_TYPE = char *;

	static void Operation(const char *input_data, idx_t start, idx_t count, RESULT_TYPE &result) {
		memcpy(result, input_data + start, count);
		result += count;
	}
	static void Operation(const char *input_data, RESULT_TYPE &result) {
		Operation(input_data, 0, strlen(input_data), result);
	}
};

struct Matcher {
	template <class OP>
	static void Decode(string_t &input, UnescapeBindData &info, typename OP::RESULT_TYPE &result) {
		auto input_data = input.GetData();
		auto input_size = input.GetSize();
		uint32_t num = 0; // the interpretation of the input string to the actual number
		idx_t start = 0;  // match's start
		MatchType type = MatchType::Unknown;
		for (idx_t i = 0; i < input_size; ++i) {
			if (type != MatchType::Unknown) {
				if (input_data[i] == '&') { // the beginning of a new match
					DecodeMatch<OP>(type, input_data, start, i - 1, num, result, info);
					num = 0;
					type = GetMatchType(input, i, start);
				} else if (input_data[i] == ';') { // match ends with semicolon
					DecodeMatch<OP>(type, input_data, start, i, num, result, info);
					type = MatchType::Unknown;
				} else { // check if the digit is valid for decoding
					bool is_valid = true;
					if (type == MatchType::Decimal) {
						is_valid = CheckDecDigit(input_data, i, num);
					} else if (type == MatchType::Hex) {
						is_valid = CheckHexDigit(input_data, i, num);
					}
					if (!is_valid) {
						if (start == i) {
							DecodeMatch<OP>(MatchType::Unknown, input_data, start - idx_t(type), i, 0, result, info);
						} else {
							DecodeMatch<OP>(type, input_data, start, i - 1, num, result, info);
							OP::Operation(input_data, i, 1, result);
						}
						type = MatchType::Unknown;
					}
				}
			} else {
				// declare a match
				num = 0;
				type = GetMatchType(input, i, start);
				if (type == MatchType::Unknown) { // append the character in the result
					OP::Operation(input_data, i, 1, result);
				}
			}
		}
		if (type != MatchType::Unknown) {
			DecodeMatch<OP>(type, input_data, start, input_size - 1, num, result, info);
		}
	}

	static MatchType GetMatchType(string_t input, idx_t &i, idx_t &start) {
		MatchType type;
		auto input_data = input.GetData();
		auto input_size = input.GetSize();
		auto num_of_digits = input_size - i;             // number of digits after i index
		if (num_of_digits > 2 && input_data[i] == '&') { // the beggining of a match
			if (input_data[i + 1] == '#') {
				if (input_data[i + 2] == 'x' || input_data[i + 2] == 'X') {
					if (num_of_digits == 3 || input_data[i + 3] == '&' || input_data[i + 3] == ';') {
						return MatchType::Unknown;
					}
					type = MatchType::Hex;
					i += 2; // point to the x digit
				} else {
					type = MatchType::Decimal;
					i += 1; // point to the # digit
				}
			} else {
				type = MatchType::CharRef;
			}
			start = i + 1;
			return type;
		}
		return MatchType::Unknown;
	}

	// append to the result
	template <class OP>
	static void Append(const char *input_data, idx_t start, idx_t end, typename OP::RESULT_TYPE &result) {
		OP::Operation(input_data, start, end - start + 1, result);
	}

	template <class OP>
	static void DecodeMatch(MatchType type, const char *input_data, idx_t start, idx_t end, uint32_t num,
	                        typename OP::RESULT_TYPE &result, UnescapeBindData &info, bool semicolon = false) {
		if (type == MatchType::Unknown) {
			Append<OP>(input_data, start, end, result);
		} else if (type == MatchType::CharRef) {
			AppendCharRefMatch<OP>(input_data, start, end, result, info);
		} else { // MatchType::Decimal || MatchType::Hex
			AppendUnicodeMatch<OP>(num, result, info);
		}
	}

	// decode unicode matches (decimal or hexadecimal)
	template <class OP>
	static void AppendUnicodeMatch(uint32_t num, typename OP::RESULT_TYPE &result, UnescapeBindData &bind_data) {
		// line feed character
		if (num == 0x0d) {
			OP::Operation("\\r", result);
			return;
		}
		// return the replacement character
		if (num == 0x00 || (0xD800 <= num && num <= 0xDFFF) || 0x10FFFF < num) {
			OP::Operation("\uFFFD", result);
			return;
		}
		// special character references
		if (0x80 <= num && num <= 0x9f) {
			auto invaid_charref = bind_data.special_invalid_charrefs.find(num);
			if (invaid_charref != bind_data.special_invalid_charrefs.end()) {
				auto ch = invaid_charref->second;
				OP::Operation(ch, result);
			} else { // non printable control points
				if (num == 0x81) {
					OP::Operation("\\x81", result);
				} else if (num == 0x8d) {
					OP::Operation("\\x8d", result);
				} else if (num == 0x8f) {
					OP::Operation("\\x8f", result);
				} else if (num == 0x90) {
					OP::Operation("\\x90", result);
				} else if (num == 0x9d) {
					OP::Operation("\\x9d", result);
				} else {
					throw InternalException("Tried to decode contol point %d, but it was not handled", num);
				}
			}
			return;
		}
		if (bind_data.invalid_codepoints.find(num) != bind_data.invalid_codepoints.end()) {
			// skip this character
			return;
		}
		int utf8_sz = 0;
		char utf8_val[4] = {'\0', '\0', '\0', '\0'};
		if (!Utf8Proc::CodepointToUtf8(num, utf8_sz, utf8_val)) {
			throw InternalException("Cannot convert codepoint %d", num);
		}
		Append<OP>(utf8_val, 0, utf8_sz - 1, result);
	}

	// decode character reference - looks for the biggest valid charref in the match
	template <class OP>
	static void AppendCharRefMatch(const char *input_data, idx_t start, idx_t end, typename OP::RESULT_TYPE &result,
	                               UnescapeBindData &bind_data) {
		idx_t sz = end - start + 1;
		if (sz < 33) { // the biggest valid character reference has 32 characters
			for (; sz >= 1; --sz) {
				auto it = bind_data.html5_names.mapped_strings.find(string_t(input_data + start, sz));
				if (it != bind_data.html5_names.mapped_strings.end()) {
					char glyph[8] = {};
					// encode first codepoint
					int utf8_sz = 0;
					if (!Utf8Proc::CodepointToUtf8(it->second.codepoints[0], utf8_sz, glyph)) {
						throw InternalException("Cannot encode to utf8 the first codepoint of &%s",
						                        it->first.GetString());
					}
					// encode second codepoint (if it exists)
					int utf8_sz2 = 0;
					if (it->second.codepoints[1] != 0 &&
					    !Utf8Proc::CodepointToUtf8(it->second.codepoints[1], utf8_sz2, glyph + utf8_sz)) {
						throw InternalException("Cannot encode to utf8 the second codepoint of &%s",
						                        it->first.GetString());
					}
					Append<OP>(glyph, 0, utf8_sz + utf8_sz2 - 1, result);
					Append<OP>(input_data, start + sz, end, result); // append the rest of the match
					return;
				}
			}
		}
		Append<OP>(input_data, start - 1, end, result); // return the whole match
	}

	static bool CheckDecDigit(const char *input_data, idx_t pos, uint32_t &num) {
		int dec_digit = Blob::HEX_MAP[static_cast<unsigned char>(input_data[pos])];
		if (-1 < dec_digit && dec_digit < 10) {
			if (num * 10 < (UINT32_MAX - dec_digit) / 10) { // prevent UINT32_MAX overflowing
				num *= 10;
				num += dec_digit;
			}
			return true;
		}
		return false;
	}

	static bool CheckHexDigit(const char *input_data, idx_t pos, uint32_t &num) {
		int hex_digit = Blob::HEX_MAP[static_cast<unsigned char>(input_data[pos])];
		if (hex_digit > -1) {
			if (num * 16 < (UINT32_MAX - hex_digit) / 16) { // prevent UINT32_MAX overflowing
				num *= 16;
				num += hex_digit;
			}
			return true;
		}
		return false;
	}
};

static string_t EscapeInputStr(string_t &input_str, bool input_quote, Vector &result) {
	constexpr idx_t QUOTE_SZ = 1;
	constexpr idx_t AMPERSAND_SZ = 5;
	constexpr idx_t ANGLE_BRACKET_SZ = 4;
	constexpr idx_t TRANSLATED_QUOTE_SZ = 6; // e.g. \" is translated to &quot;, \' is translated to &#x27;

	const auto input_data = input_str.GetData();
	const auto input_size = input_str.GetSize();

	auto calc_sz = [&]() {
		idx_t sz = 0;
		for (idx_t i = 0; i < input_size; i++) {
			char ch = input_data[i];
			if (ch == '&') {
				sz += AMPERSAND_SZ;
			} else if (ch == '<' || ch == '>') {
				sz += ANGLE_BRACKET_SZ;
			} else if (ch == '\"' || ch == '\'') {
				sz += (input_quote) ? TRANSLATED_QUOTE_SZ : QUOTE_SZ;
			} else {
				sz++;
			}
		}
		return sz;
	};

	auto result_size = calc_sz();
	auto result_string = StringVector::EmptyString(result, result_size);
	auto result_data = result_string.GetDataWriteable();

	idx_t index = 0;
	for (idx_t i = 0; i < input_size; ++i) {
		char ch = input_data[i];
		switch (ch) {
		case '&':
			memcpy(result_data + index, "&amp;", AMPERSAND_SZ);
			index += AMPERSAND_SZ;
			break;
		case '<':
			memcpy(result_data + index, "&lt;", ANGLE_BRACKET_SZ);
			index += ANGLE_BRACKET_SZ;
			break;
		case '>':
			memcpy(result_data + index, "&gt;", ANGLE_BRACKET_SZ);
			index += ANGLE_BRACKET_SZ;
			break;
		case '"':
			if (input_quote) {
				memcpy(result_data + index, "&quot;", TRANSLATED_QUOTE_SZ);
				index += TRANSLATED_QUOTE_SZ;
			} else {
				memcpy(result_data + index, "\"", QUOTE_SZ);
				index += QUOTE_SZ;
			}
			break;
		case '\'':
			if (input_quote) {
				memcpy(result_data + index, "&#x27;", TRANSLATED_QUOTE_SZ);
				index += TRANSLATED_QUOTE_SZ;
			} else {
				memcpy(result_data + index, "\'", QUOTE_SZ);
				index += QUOTE_SZ;
			}
			break;
		default:
			// add input character without translation
			memcpy(result_data + index, &ch, 1);
			index += 1;
		}
	}
	result_string.Finalize();
	return result_string;
}

void INetFunctions::Escape(DataChunk &args, ExpressionState &state, Vector &result) {
	if (args.ColumnCount() == 1) {
		UnaryExecutor::Execute<string_t, string_t>(args.data[0], result, args.size(), [&](string_t input_str) {
			return EscapeInputStr(input_str, true, result);
		});
	} else {
		BinaryExecutor::Execute<string_t, bool, string_t>(
		    args.data[0], args.data[1], result, args.size(),
		    [&](string_t input_str, bool input_quote) { return EscapeInputStr(input_str, input_quote, result); });
	}
}

void INetFunctions::Unescape(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	auto &info = func_expr.bind_info->Cast<UnescapeBindData>();
	UnaryExecutor::Execute<string_t, string_t>(args.data[0], result, args.size(), [&](string_t input_st) {
		idx_t result_size = 0;
		Matcher::Decode<MatchLengthCounter>(input_st, info, result_size);

		auto result_string = StringVector::EmptyString(result, result_size);
		auto result_data = result_string.GetDataWriteable();
		Matcher::Decode<MatchWriter>(input_st, info, result_data);
		result_string.Finalize();
		return result_string;
	});
}

ScalarFunctionSet InetExtension::GetEscapeFunctionSet() {
	ScalarFunctionSet funcs("html_escape");
	funcs.AddFunction(ScalarFunction({LogicalType::VARCHAR}, LogicalType::VARCHAR, INetFunctions::Escape));
	funcs.AddFunction(
	    ScalarFunction({LogicalType::VARCHAR, LogicalType::BOOLEAN}, LogicalType::VARCHAR, INetFunctions::Escape));
	return funcs;
}

ScalarFunction InetExtension::GetUnescapeFunction() {
	return ScalarFunction("html_unescape", {LogicalType::VARCHAR}, LogicalType::VARCHAR, INetFunctions::Unescape,
	                      UnescapeBind);
}

} // namespace duckdb
