#pragma once
#include "utf8proc_wrapper.hpp"
#include "duckdb/common/arena_linked_list.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/types/blob.hpp"
#include "duckdb/common/types/string_type.hpp"
#include "duckdb/parser/expression/cast_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/parser/peg/special_string_utils.hpp"
#include "duckdb/parser/peg/token_type.hpp"
#include "duckdb/common/windows_undefs.hpp"
#include <type_traits>

namespace duckdb {

inline string TokenTypeToString(TokenType type) {
	switch (type) {
	case TokenType::KEYWORD:
		return "KEYWORD";
	case TokenType::STRING_LITERAL:
		return "STRING_LITERAL";
	case TokenType::NUMBER_LITERAL:
		return "NUMBER_LITERAL";
	case TokenType::OPERATOR:
		return "OPERATOR";
	case TokenType::IDENTIFIER:
		return "IDENTIFIER";
	case TokenType::COMMENT:
		return "COMMENT";
	case TokenType::TERMINATOR:
		return "TERMINATOR";
	case TokenType::TOKEN_ERROR:
		return "ERROR";
	case TokenType::CATALOG_NAME:
		return "CATALOG_NAME";
	case TokenType::SCHEMA_NAME:
		return "SCHEMA_NAME";
	case TokenType::TABLE_NAME:
		return "TABLE_NAME";
	case TokenType::TYPE_NAME:
		return "TYPE_NAME";
	case TokenType::COLUMN_NAME:
		return "COLUMN_NAME";
	case TokenType::SCALAR_FUNCTION:
		return "SCALAR_FUNCTION";
	case TokenType::TABLE_FUNCTION:
		return "TABLE_FUNCTION";
	case TokenType::PRAGMA_FUNCTION:
		return "PRAGMA_FUNCTION";
	case TokenType::SETTING_NAME:
		return "SETTING_NAME";
	case TokenType::END_OF_INPUT:
		return "END_OF_INPUT";
	case TokenType::END_OF_INPUT_AUTOCOMPLETE:
		return "END_OF_INPUT_AUTOCOMPLETE";
	default:
		return "UNKNOWN";
	}
}

class PEGTransformer; // Forward declaration
class ParseResultAllocator;
struct CompiledGrammarRule;

class ParseResultRef {
public:
	ParseResultRef() = default;

	bool IsValid() const {
		return index != INVALID_INDEX;
	}

	uint32_t GetIndex() const {
		D_ASSERT(IsValid());
		return index;
	}

	bool operator==(const ParseResultRef &other) const {
		return index == other.index;
	}

	bool operator!=(const ParseResultRef &other) const {
		return !(*this == other);
	}

private:
	static constexpr uint32_t INVALID_INDEX = NumericLimits<uint32_t>::Maximum();
	explicit ParseResultRef(uint32_t index_p) : index(index_p) {
	}

private:
	friend class ParseResultAllocator;
	uint32_t index = INVALID_INDEX;
};

struct ParseResultRange {
	uint32_t offset = 0;
	uint32_t count = 0;
};

enum class ParseResultType : uint8_t {
	LIST,
	OPTIONAL,
	REPEAT,
	CHOICE,
	EXPRESSION,
	IDENTIFIER,
	KEYWORD,
	OPERATOR,
	STATEMENT,
	EXTENSION,
	NUMBER,
	STRING,
	END_OF_INPUT,
	INVALID
};

inline const char *ParseResultToString(ParseResultType type) {
	switch (type) {
	case ParseResultType::LIST:
		return "LIST";
	case ParseResultType::OPTIONAL:
		return "OPTIONAL";
	case ParseResultType::REPEAT:
		return "REPEAT";
	case ParseResultType::CHOICE:
		return "CHOICE";
	case ParseResultType::EXPRESSION:
		return "EXPRESSION";
	case ParseResultType::IDENTIFIER:
		return "IDENTIFIER";
	case ParseResultType::KEYWORD:
		return "KEYWORD";
	case ParseResultType::OPERATOR:
		return "OPERATOR";
	case ParseResultType::STATEMENT:
		return "STATEMENT";
	case ParseResultType::EXTENSION:
		return "EXTENSION";
	case ParseResultType::NUMBER:
		return "NUMBER";
	case ParseResultType::STRING:
		return "STRING";
	case ParseResultType::END_OF_INPUT:
		return "END_OF_INPUT";
	case ParseResultType::INVALID:
		return "INVALID";
	}
	return "INVALID";
}

class ParseResult {
public:
	explicit ParseResult(ParseResultAllocator &allocator_p, ParseResultType type, optional_idx offset,
	                     optional_idx length = optional_idx())
	    : allocator(allocator_p), type(type), offset(offset), length(length) {
	}
	virtual ~ParseResult() = default;

	ParseResult(const ParseResult &) = delete;
	ParseResult &operator=(const ParseResult &) = delete;
	ParseResult(ParseResult &&) = default;
	ParseResult &operator=(ParseResult &&) = delete;

	template <class TARGET>
	TARGET &Cast() {
		if (TARGET::TYPE != ParseResultType::INVALID && type != TARGET::TYPE) {
			throw InternalException("Failed to cast parse result of type %s to type %s for rule %s",
			                        ParseResultToString(TARGET::TYPE), ParseResultToString(type), name);
		}
		return reinterpret_cast<TARGET &>(*this);
	}

	ParseResultAllocator &allocator;
	ParseResultRef result_reference;
	ParseResultType type;
	string name;
	optional_ptr<const CompiledGrammarRule> rule;
	optional_idx offset;
	//! Source length: for leaf tokens the token length; for composite results the enclosing extent of children
	optional_idx length;

	void SetRule(const CompiledGrammarRule &rule_p) {
		rule = rule_p;
	}
	optional_ptr<const CompiledGrammarRule> GetRule() const {
		return rule;
	}
	ParseResultRef GetReference() const {
		D_ASSERT(result_reference.IsValid());
		return result_reference;
	}

	//! Returns the source location [offset, offset+length) of this parse result (length 0 when unknown)
	QueryLocation GetLocation() const {
		if (!offset.IsValid()) {
			return QueryLocation();
		}
		return QueryLocation(offset.GetIndex(), length.IsValid() ? length.GetIndex() : 0);
	}

	//! Grow this result's location so it encloses the given child's location (used to build composite
	//! locations bottom-up)
	void EncloseChild(const ParseResult &child) {
		auto child_location = child.GetLocation();
		if (!offset.IsValid() || !child_location.IsValid()) {
			return;
		}
		auto enclosing = GetLocation().Merge(child_location);
		length = enclosing.length;
	}

	virtual void ToStringInternal(std::stringstream &ss, std::unordered_set<const ParseResult *> &visited,
	                              const std::string &indent, bool is_last) const {
		ss << indent << (is_last ? "└─" : "├─") << " " << ParseResultToString(type);
		if (!name.empty()) {
			ss << " (" << name << ")";
		}
	}

	// The public entry point
	std::string ToString() const {
		std::stringstream ss;
		std::unordered_set<const ParseResult *> visited;
		// The root is always the "last" element at its level
		ToStringInternal(ss, visited, "", true);
		return ss.str();
	}
};

struct IdentifierParseResult : ParseResult {
	static constexpr ParseResultType TYPE = ParseResultType::IDENTIFIER;
	Identifier identifier;

	explicit IdentifierParseResult(ParseResultAllocator &allocator, string identifier_p, optional_idx offset,
	                               optional_idx length = optional_idx())
	    : ParseResult(allocator, TYPE, offset, length), identifier(std::move(identifier_p)) {
	}

	void ToStringInternal(std::stringstream &ss, std::unordered_set<const ParseResult *> &visited,
	                      const std::string &indent, bool is_last) const override {
		ParseResult::ToStringInternal(ss, visited, indent, is_last);
		ss << ": " << identifier.GetIdentifierName() << "\n";
	}
};

struct EndOfInputParseResult : ParseResult {
	static constexpr ParseResultType TYPE = ParseResultType::END_OF_INPUT;

	explicit EndOfInputParseResult(ParseResultAllocator &allocator) : ParseResult(allocator, TYPE, optional_idx()) {
	}

	void ToStringInternal(std::stringstream &ss, std::unordered_set<const ParseResult *> &visited,
	                      const std::string &indent, bool is_last) const override {
		ParseResult::ToStringInternal(ss, visited, indent, is_last);
		ss << "\n";
	}
};

struct KeywordParseResult : ParseResult {
	static constexpr ParseResultType TYPE = ParseResultType::KEYWORD;
	string keyword;

	explicit KeywordParseResult(ParseResultAllocator &allocator, string keyword_p, optional_idx offset,
	                            optional_idx length = optional_idx())
	    : ParseResult(allocator, TYPE, offset, length), keyword(std::move(keyword_p)) {
	}

	void ToStringInternal(std::stringstream &ss, std::unordered_set<const ParseResult *> &visited,
	                      const std::string &indent, bool is_last) const override {
		ParseResult::ToStringInternal(ss, visited, indent, is_last);
		ss << ": \"" << keyword << "\"\n";
	}
};

struct ListParseResult : ParseResult {
	static constexpr ParseResultType TYPE = ParseResultType::LIST;

public:
	explicit ListParseResult(ParseResultAllocator &allocator, vector<ParseResultRef> results_p, string name_p,
	                         optional_idx offset);

	vector<reference<ParseResult>> GetChildren() const;
	ParseResult &GetChild(idx_t index);

	template <class T>
	T &Child(idx_t index) {
		return GetChild(index).Cast<T>();
	}

	void ToStringInternal(std::stringstream &ss, std::unordered_set<const ParseResult *> &visited,
	                      const std::string &indent, bool is_last) const override {
		ss << indent << (is_last ? "└─" : "├─");

		if (visited.count(this)) {
			ss << " List (" << name << ") [... already printed ...]\n";
			return;
		}
		visited.insert(this);

		ss << " " << ParseResultToString(type);
		if (!name.empty()) {
			ss << " (" << name << ")";
		}
		ss << " [" << children.count << " children]\n";

		std::string child_indent = indent + (is_last ? "   " : "│  ");
		auto child_results = GetChildren();
		for (idx_t i = 0; i < child_results.size(); ++i) {
			child_results[i].get().ToStringInternal(ss, visited, child_indent, i == child_results.size() - 1);
		}
	}

private:
	ParseResultRange children;
};

struct RepeatParseResult : ParseResult {
	static constexpr ParseResultType TYPE = ParseResultType::REPEAT;

	explicit RepeatParseResult(ParseResultAllocator &allocator, vector<ParseResultRef> results_p, optional_idx offset);

	vector<reference<ParseResult>> GetChildren() const;

	template <class T>
	T &Child(idx_t index) {
		if (index >= children.count) {
			throw InternalException("Child index out of bounds");
		}
		return GetChild(index).Cast<T>();
	}

	ParseResult &GetChild(idx_t index);

	void ToStringInternal(std::stringstream &ss, std::unordered_set<const ParseResult *> &visited,
	                      const std::string &indent, bool is_last) const override {
		ss << indent << (is_last ? "└─" : "├─");

		if (visited.count(this)) {
			ss << " Repeat (" << name << ") [... already printed ...]\n";
			return;
		}
		visited.insert(this);

		ss << " " << ParseResultToString(type);
		if (!name.empty()) {
			ss << " (" << name << ")";
		}
		ss << " [" << children.count << " children]\n";

		std::string child_indent = indent + (is_last ? "   " : "│  ");
		auto child_results = GetChildren();
		for (idx_t i = 0; i < child_results.size(); ++i) {
			child_results[i].get().ToStringInternal(ss, visited, child_indent, i == child_results.size() - 1);
		}
	}

private:
	ParseResultRange children;
};

struct OptionalParseResult : ParseResult {
	static constexpr ParseResultType TYPE = ParseResultType::OPTIONAL;

	explicit OptionalParseResult(ParseResultAllocator &allocator) : ParseResult(allocator, TYPE, optional_idx()) {
	}
	explicit OptionalParseResult(ParseResultAllocator &allocator, ParseResultRef result_p, optional_idx offset);

	bool HasResult() const {
		return optional_result.IsValid();
	}

	ParseResult &GetResultUnsafe();
	const ParseResult &GetResultUnsafe() const;
	ParseResult &GetResult();
	const ParseResult &GetResult() const;

	void ToStringInternal(std::stringstream &ss, std::unordered_set<const ParseResult *> &visited,
	                      const std::string &indent, bool is_last) const override {
		if (HasResult()) {
			// The optional node has a value, so we "collapse" it by just printing its child.
			// We pass the same indentation and is_last status, so it takes the place of the Optional node.
			GetResultUnsafe().ToStringInternal(ss, visited, indent, is_last);
		} else {
			// The optional node is empty, which is useful information, so we print it.
			ss << indent << (is_last ? "└─" : "├─") << " " << ParseResultToString(type) << " [empty]\n";
		}
	}

private:
	ParseResultRef optional_result;
};

class ChoiceParseResult : public ParseResult {
public:
	static constexpr ParseResultType TYPE = ParseResultType::CHOICE;

	explicit ChoiceParseResult(ParseResultAllocator &allocator, ParseResultRef parse_result_p, idx_t selected_idx_p,
	                           optional_idx offset);

	ParseResult &GetResult();
	const ParseResult &GetResult() const;

	void ToStringInternal(std::stringstream &ss, std::unordered_set<const ParseResult *> &visited,
	                      const std::string &indent, bool is_last) const override {
		// The choice was resolved. We print a marker and then print the child below it.
		ss << indent << (is_last ? "└─" : "├─") << " [" << ParseResultToString(type) << " (idx: " << selected_idx
		   << ")] ->\n";

		// The child is now on a new indentation level and is the only child of our marker.
		std::string child_indent = indent + (is_last ? "   " : "│  ");
		GetResult().ToStringInternal(ss, visited, child_indent, true);
	}

private:
	ParseResultRef result;
	idx_t selected_idx;
};

class NumberParseResult : public ParseResult {
public:
	static constexpr ParseResultType TYPE = ParseResultType::NUMBER;

	explicit NumberParseResult(ParseResultAllocator &allocator, string number_p, optional_idx offset,
	                           optional_idx length = optional_idx())
	    : ParseResult(allocator, TYPE, offset, length), number(std::move(number_p)) {
	}
	string number;

	void ToStringInternal(std::stringstream &ss, std::unordered_set<const ParseResult *> &visited,
	                      const std::string &indent, bool is_last) const override {
		ParseResult::ToStringInternal(ss, visited, indent, is_last);
		ss << ": " << number << "\n";
	}
};

class StringLiteralParseResult : public ParseResult {
public:
	static constexpr ParseResultType TYPE = ParseResultType::STRING;

	explicit StringLiteralParseResult(ParseResultAllocator &allocator, string string_p,
	                                  SpecialStringCharacter string_type_p, optional_idx offset,
	                                  optional_idx length = optional_idx())
	    : ParseResult(allocator, TYPE, offset, length), result(std::move(string_p)), string_type(string_type_p) {
	}

	string GetRawString() const {
		return result;
	}

	unique_ptr<ParsedExpression> ToExpression() {
		switch (string_type) {
		case SpecialStringCharacter::STANDARD:
			return ConstantExpression::String(result);
		case SpecialStringCharacter::NATIONAL_STRING:
			return make_uniq<CastExpression>(LogicalType::VARCHAR, ConstantExpression::String(result));
		case SpecialStringCharacter::HEXADECIMAL_STRING:
			// result contains raw hex digits (e.g. "FF" for X'FF')
			return ConstantExpression::Hex(result);
		case SpecialStringCharacter::BIT_STRING:
			return ConstantExpression::Bit(result);
		case SpecialStringCharacter::ESCAPE_STRING:
			string escaped_result;
			escaped_result.reserve(result.size());

			for (size_t i = 0; i < result.size(); ++i) {
				if (result[i] == '\\' && i + 1 < result.size()) {
					i++;
					switch (result[i]) {
					case 'b':
						escaped_result += '\b';
						break;
					case 'f':
						escaped_result += '\f';
						break;
					case '0':
					case '1':
					case '2':
					case '3':
					case '4':
					case '5':
					case '6':
					case '7': {
						size_t oct_start = i;
						size_t oct_end = oct_start + 1;
						while (oct_end < result.size() && oct_end - oct_start < 3 && result[oct_end] >= '0' &&
						       result[oct_end] <= '7') {
							oct_end++;
						}
						string oct_str = result.substr(oct_start, oct_end - oct_start);
						escaped_result += static_cast<char>(strtoul(oct_str.c_str(), nullptr, 8));
						i = oct_end - 1;
						break;
					}
					case 'x': {
						size_t hex_start = i + 1;
						size_t hex_end = hex_start;
						while (hex_end < result.size() && hex_end - hex_start < 2 &&
						       StringUtil::CharacterIsHex(result[hex_end])) {
							hex_end++;
						}
						if (hex_end > hex_start) {
							string hex_str = result.substr(hex_start, hex_end - hex_start);
							escaped_result += static_cast<char>(strtoul(hex_str.c_str(), nullptr, 16));
							i = hex_end - 1;
						} else {
							escaped_result += 'x';
						}
						break;
					}
					case 'n':
						escaped_result += '\n';
						break;
					case 't':
						escaped_result += '\t';
						break;
					case 'r':
						escaped_result += '\r';
						break;
					case '\\':
						escaped_result += '\\';
						break;
					case '\'':
						escaped_result += '\'';
						break;
					default:
						escaped_result += result[i];
						break;
					}
				} else {
					escaped_result += result[i];
				}
			}
			if (escaped_result.find('\0') != string::npos) {
				throw ParserException("Null character not permitted in escape string literal");
			}
			UnicodeInvalidReason reason;
			size_t pos;
			auto utf_validity = Utf8Proc::Analyze(escaped_result.c_str(), escaped_result.size(), &reason, &pos);
			if (utf_validity == UnicodeType::INVALID) {
				const char *reason_str =
				    reason == UnicodeInvalidReason::BYTE_MISMATCH ? "byte mismatch" : "invalid unicode codepoint";
				throw ParserException("Invalid UTF-8 in escape string literal at byte offset %d: %s", pos, reason_str);
			}
			return ConstantExpression::String(escaped_result);
		}
		return ConstantExpression::String(result);
	}

	string result;

	SpecialStringCharacter string_type;

	void ToStringInternal(std::stringstream &ss, std::unordered_set<const ParseResult *> &visited,
	                      const std::string &indent, bool is_last) const override {
		ParseResult::ToStringInternal(ss, visited, indent, is_last);
		string special_string;
		if (string_type == SpecialStringCharacter::ESCAPE_STRING) {
			special_string = "E";
		} else if (string_type == SpecialStringCharacter::NATIONAL_STRING) {
			special_string = "N";
		} else if (string_type == SpecialStringCharacter::HEXADECIMAL_STRING) {
			special_string = "X";
		}
		ss << ": " << special_string << "\"" << result << "\"n";
	}
};

class OperatorParseResult : public ParseResult {
public:
	static constexpr ParseResultType TYPE = ParseResultType::OPERATOR;

	explicit OperatorParseResult(ParseResultAllocator &allocator, string operator_p, optional_idx offset,
	                             optional_idx length = optional_idx())
	    : ParseResult(allocator, TYPE, offset, length), operator_token(std::move(operator_p)) {
	}
	string operator_token;

	void ToStringInternal(std::stringstream &ss, std::unordered_set<const ParseResult *> &visited,
	                      const std::string &indent, bool is_last) const override {
		ParseResult::ToStringInternal(ss, visited, indent, is_last);
		ss << ": " << operator_token << "\n";
	}
};

template <class T>
struct IsBuiltinParseResult
    : std::disjunction<std::is_same<T, IdentifierParseResult>, std::is_same<T, EndOfInputParseResult>,
                       std::is_same<T, KeywordParseResult>, std::is_same<T, ListParseResult>,
                       std::is_same<T, RepeatParseResult>, std::is_same<T, OptionalParseResult>,
                       std::is_same<T, ChoiceParseResult>, std::is_same<T, NumberParseResult>,
                       std::is_same<T, StringLiteralParseResult>, std::is_same<T, OperatorParseResult>> {};

class ParseResultSlot {
public:
	template <class RESULT, class... ARGS>
	explicit ParseResultSlot(std::in_place_type_t<RESULT>, ParseResultAllocator &allocator, ARGS &&... args) {
		static_assert(IsBuiltinParseResult<RESULT>::value, "Only built-in parse results can be flattened");
		new (&storage) RESULT(allocator, std::forward<ARGS>(args)...);
	}

	ParseResultSlot(ParseResultSlot &&other) {
		MoveFrom(other);
	}

	ParseResultSlot(const ParseResultSlot &) = delete;
	ParseResultSlot &operator=(const ParseResultSlot &) = delete;
	ParseResultSlot &operator=(ParseResultSlot &&) = delete;

	~ParseResultSlot() {
		Get().~ParseResult();
	}

	ParseResult &Get() {
		return *reinterpret_cast<ParseResult *>(&storage);
	}

	const ParseResult &Get() const {
		return *reinterpret_cast<const ParseResult *>(&storage);
	}

private:
	void MoveFrom(ParseResultSlot &other) {
		auto &result = other.Get();
		switch (result.type) {
		case ParseResultType::IDENTIFIER:
			new (&storage) IdentifierParseResult(std::move(result.Cast<IdentifierParseResult>()));
			break;
		case ParseResultType::END_OF_INPUT:
			new (&storage) EndOfInputParseResult(std::move(result.Cast<EndOfInputParseResult>()));
			break;
		case ParseResultType::KEYWORD:
			new (&storage) KeywordParseResult(std::move(result.Cast<KeywordParseResult>()));
			break;
		case ParseResultType::LIST:
			new (&storage) ListParseResult(std::move(result.Cast<ListParseResult>()));
			break;
		case ParseResultType::REPEAT:
			new (&storage) RepeatParseResult(std::move(result.Cast<RepeatParseResult>()));
			break;
		case ParseResultType::OPTIONAL:
			new (&storage) OptionalParseResult(std::move(result.Cast<OptionalParseResult>()));
			break;
		case ParseResultType::CHOICE:
			new (&storage) ChoiceParseResult(std::move(result.Cast<ChoiceParseResult>()));
			break;
		case ParseResultType::NUMBER:
			new (&storage) NumberParseResult(std::move(result.Cast<NumberParseResult>()));
			break;
		case ParseResultType::STRING:
			new (&storage) StringLiteralParseResult(std::move(result.Cast<StringLiteralParseResult>()));
			break;
		case ParseResultType::OPERATOR:
			new (&storage) OperatorParseResult(std::move(result.Cast<OperatorParseResult>()));
			break;
		default:
			throw InternalException("Unsupported parse result type in flattened pool");
		}
	}

private:
	using storage_t =
	    typename std::aligned_union<0, IdentifierParseResult, EndOfInputParseResult, KeywordParseResult,
	                                ListParseResult, RepeatParseResult, OptionalParseResult, ChoiceParseResult,
	                                NumberParseResult, StringLiteralParseResult, OperatorParseResult>::type;
	storage_t storage;
};

class ParseResultAllocator {
public:
	template <class RESULT, class... ARGS>
	ParseResultRef Allocate(ARGS &&... args) {
		static_assert(IsBuiltinParseResult<RESULT>::value, "Only built-in parse results can be flattened");
		if (parse_results.size() >= NumericLimits<uint32_t>::Maximum()) {
			throw InternalException("Too many parse results");
		}
		auto result = ParseResultRef(NumericCast<uint32_t>(parse_results.size()));
		parse_results.emplace_back(std::in_place_type<RESULT>, *this, std::forward<ARGS>(args)...);
		Get(result).result_reference = result;
		return result;
	}

	ParseResult &Get(ParseResultRef result) {
		if (!result.IsValid() || result.GetIndex() >= parse_results.size()) {
			throw InternalException("Parse result index out of bounds");
		}
		return parse_results[result.GetIndex()].Get();
	}

	const ParseResult &Get(ParseResultRef result) const {
		if (!result.IsValid() || result.GetIndex() >= parse_results.size()) {
			throw InternalException("Parse result index out of bounds");
		}
		return parse_results[result.GetIndex()].Get();
	}

	idx_t Count() const {
		return parse_results.size();
	}

private:
	ParseResultRange StoreChildren(const vector<ParseResultRef> &results) {
		if (child_results.size() + results.size() > NumericLimits<uint32_t>::Maximum()) {
			throw InternalException("Too many parse result children");
		}
		ParseResultRange range {NumericCast<uint32_t>(child_results.size()), NumericCast<uint32_t>(results.size())};
		for (auto result : results) {
			if (!result.IsValid() || result.GetIndex() >= parse_results.size()) {
				throw InternalException("Parse result child must precede its parent");
			}
			child_results.push_back(result);
		}
		return range;
	}

	ParseResult &GetChild(ParseResultRange range, idx_t index) {
		if (index >= range.count || idx_t(range.offset) + index >= child_results.size()) {
			throw InternalException("Parse result child index out of bounds");
		}
		return Get(child_results[range.offset + index]);
	}

	vector<reference<ParseResult>> GetChildren(ParseResultRange range) {
		vector<reference<ParseResult>> result;
		result.reserve(range.count);
		for (idx_t index = 0; index < range.count; index++) {
			result.push_back(GetChild(range, index));
		}
		return result;
	}

private:
	friend struct ListParseResult;
	friend struct RepeatParseResult;
	friend struct OptionalParseResult;
	friend class ChoiceParseResult;
	vector<ParseResultSlot> parse_results;
	vector<ParseResultRef> child_results;
};

inline ListParseResult::ListParseResult(ParseResultAllocator &allocator, vector<ParseResultRef> results_p,
                                        string name_p, optional_idx offset)
    : ParseResult(allocator, TYPE, offset), children(allocator.StoreChildren(results_p)) {
	name = std::move(name_p);
	for (auto child : results_p) {
		EncloseChild(allocator.Get(child));
	}
}

inline vector<reference<ParseResult>> ListParseResult::GetChildren() const {
	return allocator.GetChildren(children);
}

inline ParseResult &ListParseResult::GetChild(idx_t index) {
	return allocator.GetChild(children, index);
}

inline RepeatParseResult::RepeatParseResult(ParseResultAllocator &allocator, vector<ParseResultRef> results_p,
                                            optional_idx offset)
    : ParseResult(allocator, TYPE, offset), children(allocator.StoreChildren(results_p)) {
	for (auto child : results_p) {
		EncloseChild(allocator.Get(child));
	}
}

inline vector<reference<ParseResult>> RepeatParseResult::GetChildren() const {
	return allocator.GetChildren(children);
}

inline ParseResult &RepeatParseResult::GetChild(idx_t index) {
	return allocator.GetChild(children, index);
}

inline ParseResult &OptionalParseResult::GetResultUnsafe() {
	D_ASSERT(optional_result.IsValid());
	return allocator.Get(optional_result);
}

inline OptionalParseResult::OptionalParseResult(ParseResultAllocator &allocator, ParseResultRef result_p,
                                                optional_idx offset)
    : ParseResult(allocator, TYPE, offset), optional_result(result_p) {
	auto &result = allocator.Get(result_p);
	name = result.name;
	EncloseChild(result);
}

inline const ParseResult &OptionalParseResult::GetResultUnsafe() const {
	D_ASSERT(optional_result.IsValid());
	return allocator.Get(optional_result);
}

inline ParseResult &OptionalParseResult::GetResult() {
	if (!optional_result.IsValid()) {
		throw InternalException("OptionalParseResult is null");
	}
	return allocator.Get(optional_result);
}

inline const ParseResult &OptionalParseResult::GetResult() const {
	if (!optional_result.IsValid()) {
		throw InternalException("OptionalParseResult is null");
	}
	return allocator.Get(optional_result);
}

inline ChoiceParseResult::ChoiceParseResult(ParseResultAllocator &allocator, ParseResultRef parse_result_p,
                                            idx_t selected_idx_p, optional_idx offset)
    : ParseResult(allocator, TYPE, offset), result(parse_result_p), selected_idx(selected_idx_p) {
	auto &parse_result = allocator.Get(parse_result_p);
	name = parse_result.name;
	EncloseChild(parse_result);
}

inline ParseResult &ChoiceParseResult::GetResult() {
	return allocator.Get(result);
}

inline const ParseResult &ChoiceParseResult::GetResult() const {
	return allocator.Get(result);
}

} // namespace duckdb
