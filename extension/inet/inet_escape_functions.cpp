#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/common/types/blob.hpp"
#include "duckdb/main/extension_util.hpp"
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

static idx_t to_utf8(uint32_t code_point, char arr[4]) {
	int sz = 0;
	if (code_point <= 0x7F) {
		arr[0] = static_cast<char>(code_point);
		sz = 1;
	} else if (code_point <= 0x7FF) {
		arr[0] = static_cast<char>(0xC0 | ((code_point >> 6) & 0x1F));
		arr[1] = static_cast<char>(0x80 | (code_point & 0x3F));
		sz = 2;
	} else if (code_point <= 0xFFFF) {
		arr[0] = static_cast<char>(0xE0 | ((code_point >> 12) & 0x0F));
		arr[1] = static_cast<char>(0x80 | ((code_point >> 6) & 0x3F));
		arr[2] = static_cast<char>(0x80 | (code_point & 0x3F));
		sz = 3;
	} else if (code_point <= 0x10FFFF) {
		arr[0] = static_cast<char>(0xF0 | ((code_point >> 18) & 0x07));
		arr[1] = static_cast<char>(0x80 | ((code_point >> 12) & 0x3F));
		arr[2] = static_cast<char>(0x80 | ((code_point >> 6) & 0x3F));
		arr[3] = static_cast<char>(0x80 | (code_point & 0x3F));
		sz = 4;
	} else {
		throw InternalException("Cannot convert codepoint: out of range ");
	}
	return sz;
};

class Match {
public:
	Match(idx_t beg, idx_t len, bool semi) : start(beg), size(len), semicolon(semi), next(nullptr) {
	}

public:
	string_t decoded;
	idx_t start = 0;
	idx_t size = 0;
	bool semicolon = false;
	Match *next = nullptr;
};

struct UnicodeValue {
	static bool check_and_cast_input(Match *match, const char *input_data, int32_t &num,
	                                 idx_t &num_of_chars_to_decode) {
		auto start = match->start;
		auto size = match->size;
		for (idx_t i = start + 1; i < start + size; i++) {
			int digit = Blob::HEX_MAP[static_cast<unsigned char>(input_data[i])];
			if (digit < 0 || 10 < digit) { // unicode points should not have character digits
				num_of_chars_to_decode = i - start - 1;
				return false;
			}
			num *= 10;
			num += digit;
		}
		return false;
	}
};

struct HexValue {
	static bool check_and_cast_input(Match *match, const char *input_data, int32_t &num,
	                                 idx_t &num_of_chars_to_decode) {
		if (is_control_point(match, input_data)) {
			return true;
		}
		auto start = match->start;
		auto size = match->size;
		for (idx_t i = start + 2; i < start + size; i++) {
			int hex_digit = Blob::HEX_MAP[static_cast<unsigned char>(input_data[i])];
			if (hex_digit < 0) { // true if it's not a valid hexadecimal digit
				num_of_chars_to_decode = i - start - 1;
				return false;
			} else {
				// overflow - hexadecimal value too large to fit in an 32-bit int
				if (num * 16 > (INT32_MAX - hex_digit) / 16) {
					match->decoded = string_t("\uFFFD"); // return the replacement char
					return true;
				}
				num *= 16;
				num += hex_digit;
			}
		}
		return false;
	}

	// control points are not printable characters
	static bool is_control_point(Match *match, const char *input_data) {
		auto size = match->size;
		if (size != 4) {
			return false;
		}

		auto start = match->start;
		auto decode_control_point = [&]() {
			char *hex_input_data = new char[4];
			hex_input_data[0] = '\\';
			hex_input_data[1] = input_data[start + 1];
			hex_input_data[2] = input_data[start + 2];
			hex_input_data[3] = input_data[start + 3];
			match->decoded = string_t(hex_input_data, 4);
			delete[] hex_input_data;
		};

		if (input_data[start + 2] == '8') { // 0x81, 0x8d, 0x8D, 0x8f, 0x8F
			if (input_data[start + 3] == '1' || input_data[start + 3] == 'd' || input_data[start + 3] == 'D' ||
			    input_data[start + 3] == 'f' || input_data[start + 3] == 'F') {
				decode_control_point();
				return true;
			}
		}
		if (input_data[start + 2] == '9') { // 0x90, 0x9d, 0x9D
			if (input_data[start + 3] == '0' || input_data[start + 3] == 'd' || input_data[start + 3] == 'D') {
				decode_control_point();
				return true;
			}
		}
		return false;
	}
};

template <class OP>
class HexMatch : public Match {
public:
	HexMatch(const char *input_data, UnescapeBindData &bind_data, idx_t beg, idx_t len, bool semi)
	    : Match(beg, len, semi) {
		if (OP::check_and_cast_input(this, input_data, num, num_of_chars_to_decode)) {
			// true, if input is a special case and the value has been already decoded
			return;
		}
		// no need for decoding
		if (num == 0 && num_of_chars_to_decode != size - 1) {
			decoded = string_t(input_data + start - 1, size + semi + 1);
			return;
		}
		decode_match(bind_data, decoded);

		// add the rest of the match (if exists) e.g. &#8b; -> valid_part_to_decode(&#8) + add_rest('b;')
		if (size - 1 != num_of_chars_to_decode) {
			include_the_rest(input_data);
		}
	}

private:
	void decode_match(UnescapeBindData &bind_data, string_t &decoded) {
		// line feed character
		if (num == 0x0d) {
			decoded = string_t("\\r");
			return;
		}
		// return the replacement character
		if (num == 0x00 || (0xD800 <= num && num <= 0xDFFF) || 0x10FFFF < num) {
			decoded = string_t("\uFFFD");
			return;
		}
		// special character references (control points have been already handled at this point)
		if (0x80 <= num && num <= 0x9f) {
			D_ASSERT(bind_data.special_invalid_charrefs.find(num) != bind_data.special_invalid_charrefs.end());
			auto ch = bind_data.special_invalid_charrefs.at(num);
			decoded = string_t(ch);
			return;
		}
		// skip invalid codepoints
		if (bind_data.invalid_codepoints.find(num) != bind_data.invalid_codepoints.end()) {
			decoded = string_t("", 0);
			return;
		}
		char utf8_val[4] = {'\0', '\0', '\0', '\0'};
		idx_t sz = to_utf8(num, utf8_val);
		decoded = string_t(utf8_val, sz);
	}

	void include_the_rest(const char *input_data) {
		idx_t new_size = decoded.GetSize() + (size + semicolon - num_of_chars_to_decode - 1);
		char *final_result = new char[new_size];
		if (decoded.GetSize() != 0) {
			memcpy(final_result, decoded.GetData(), decoded.GetSize());
		}
		memcpy(final_result + decoded.GetSize(), input_data + start + num_of_chars_to_decode + 1,
		       size + semicolon - num_of_chars_to_decode - 1);
		decoded = string_t(final_result, new_size);
	}

private:
	// the result from input's type casting
	int32_t num = 0;
	// the actual number of characters to be decoded from the match
	idx_t num_of_chars_to_decode = size - 1; // size - 1 to rmv #
};

class CharRefMatch : public Match {
public:
	CharRefMatch(const char *input_data, UnescapeBindData &bind_data, idx_t beg, idx_t len, bool semi)
	    : Match(beg, len, semi) {
		if (size + semicolon < 33) { // the biggest valid character reference has 32 characters
			auto it = bind_data.html5_names.mapped_strings.find(string_t(input_data + start, size + semicolon));
			if (it != bind_data.html5_names.mapped_strings.end()) {
				char utf8_val[4] = {'\0', '\0', '\0', '\0'};
				idx_t sz = to_utf8(it->second, utf8_val);
				decoded = string_t(utf8_val, sz);
				return;
			}
			// check if match has a subpart that should be decoded e.g. &notin returns ¬in
			for (idx_t x = size + semicolon - 1; x >= 1; --x) {
				it = bind_data.html5_names.mapped_strings.find(string_t(input_data + start, x));
				if (it != bind_data.html5_names.mapped_strings.end()) {
					// convert value to utf8
					char utf8_val[4] = {'\0', '\0', '\0', '\0'};
					idx_t sz = to_utf8(it->second, utf8_val);

					// include the rest of the match
					idx_t new_size = sz + (size + semicolon - x);
					char *result = new char[new_size];
					memcpy(result, utf8_val, sz);
					memcpy(result + sz, input_data + start + x, size + semicolon - x);
					decoded = string_t(result, new_size);
					return;
				}
			}
		}
		decoded = string_t(input_data + start - 1, size + semicolon + 1);
	}
};

class MatchLinkedList {
public:
	MatchLinkedList(string_t &input, UnescapeBindData &info)
	    : head(nullptr), input(input), total_chars_in_list(0), bind_data(info) {
		// parse input string
		idx_t start = 0;
		idx_t size_of_last_match = 0;
		bool match = false;
		auto input_data = input.GetData();
		auto input_size = input.GetSize();
		for (idx_t i = 0; i < input_size; ++i) {
			if (input_data[i] == '&') {
				if (match && i - start > 0) { // i-start+1 = size of chars
					size_of_last_match = i - start;
					append(start, size_of_last_match, false);
				}
				start = i + 1;
				match = true;
			} else if (input_data[i] == ';') {
				if (match && i - start > 0) {
					size_of_last_match = i - start;
					append(start, size_of_last_match, true);
				}
				match = false;
			}
		}
		if (match && input_size - start > 0) {
			size_of_last_match = input_size - start - 1;
			append(start, input_size - start, false);
		}
		if (input_size - start > 0) {
			total_chars_in_list += input_size - start - 1 - size_of_last_match;
		}
	}

	~MatchLinkedList() {
		Match *current = head;
		while (current != nullptr) {
			Match *next = current->next;
			delete current;
			current = next;
		}
	}

	string_t extract_result(Vector &result, UnescapeBindData &info) {
		if (total_chars_in_list > 0) {
			idx_t result_pos = 0;
			idx_t next_match_pos = 0;
			auto input_data = input.GetData();
			auto input_size = input.GetSize();
			auto result_string = StringVector::EmptyString(result, total_chars_in_list);
			auto result_data = result_string.GetDataWriteable();
			Match *current_match = head;
			while (current_match != nullptr) {
				D_ASSERT(current_match->start - next_match_pos > 0);
				// copy the characters infront or between the matches
				memcpy(result_data + result_pos, input_data + next_match_pos,
				       current_match->start - 1 - next_match_pos);
				result_pos += current_match->start - 1 - next_match_pos;
				next_match_pos = current_match->start + current_match->size + current_match->semicolon;

				// copy the decoded result
				memcpy(result_data + result_pos, current_match->decoded.GetData(), current_match->decoded.GetSize());
				result_pos += current_match->decoded.GetSize();
				// next match
				current_match = current_match->next;
			}
			memcpy(result_data + result_pos, input_data + next_match_pos, input_size - next_match_pos);
			result_string.Finalize();
			return result_string;
		}
		auto result_string = StringVector::EmptyString(result, 0);
		result_string.Finalize();
		return result_string;
	}

private:
	void append(idx_t start, idx_t size, bool semi) {
		D_ASSERT(start + size + semi <= input.GetSize());
		auto new_match = create_match(start, size, semi);
		if (head == nullptr) {
			head = new_match;
		} else {
			Match *current = head;
			while (current->next != nullptr) {
				current = current->next;
			}
			current->next = new_match;
		}
		total_chars_in_list += (new_match->start - 1 - size_of_last_decoding) + new_match->decoded.GetSize();
		size_of_last_decoding = new_match->start + new_match->size + new_match->semicolon;
	}

	Match *create_match(idx_t start, idx_t size, bool semi) {
		auto input_data = input.GetData();
		if (input_data[start] == '#') {
			if (size < 2 || (!isdigit(input_data[start + 1]))) { // true for matches: &, &#, &#<char>..
				if ((input_data[start + 1] == 'x' || input_data[start + 1] == 'X') && size > 2) {
					return new HexMatch<HexValue>(input_data, bind_data, start, size, semi);
				}
				auto match = new Match(start, size, semi);
				match->decoded = string_t(input_data + start - 1, size + semi + 1); // return the match
				return match;
			}
			return new HexMatch<UnicodeValue>(input_data, bind_data, start, size, semi);
		}
		return new CharRefMatch(input_data, bind_data, start, size, semi);
	}

public:
	Match *head;
	string_t &input;
	idx_t total_chars_in_list;
	UnescapeBindData &bind_data;

private:
	// this is used to calculate the total_chars_in_list
	idx_t size_of_last_decoding = 0;
};

static string_t EscapeInputStr(string_t &input_str, bool input_quote, Vector &result) {
	const auto input_data = input_str.GetData();
	const auto input_size = input_str.GetSize();

	auto calc_sz = [&]() {
		idx_t sz = 0;
		for (idx_t i = 0; i < input_size; i++) {
			char ch = input_data[i];
			if (ch == '&') {
				sz += 5;
			} else if (ch == '<' || ch == '>') {
				sz += 4;
			} else if (ch == '\"' || ch == '\'') {
				sz += (input_quote) ? 6 : 1;
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
			memcpy(result_data + index, "&amp;", 5);
			index += 5;
			break;
		case '<':
			memcpy(result_data + index, "&lt;", 4);
			index += 4;
			break;
		case '>':
			memcpy(result_data + index, "&gt;", 4);
			index += 4;
			break;
		case '"':
			if (input_quote) {
				memcpy(result_data + index, "&quot;", 6);
				index += 6;
			} else {
				memcpy(result_data + index, "\"", 1);
				index += 1;
			}
			break;
		case '\'':
			if (input_quote) {
				memcpy(result_data + index, "&#x27;", 6);
				index += 6;
			} else {
				memcpy(result_data + index, "\'", 1);
				index += 1;
			}
			break;
		default:
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
		// check whether the input contains an ampersand character. If not, return the input value unchanged.
		auto matches = make_uniq<MatchLinkedList>(input_st, info);
		if (!matches->head) {
			return StringVector::AddString(result, input_st);
		}
		return matches->extract_result(result, info);
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
