#pragma once
#include "duckdb/common/arena_linked_list.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/types/string_type.hpp"
#include "duckdb/parser/parsed_expression.hpp"

namespace duckdb {

class PEGTransformer; // Forward declaration

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
	case ParseResultType::INVALID:
		return "INVALID";
	}
	return "INVALID";
}

class ParseResult {
public:
	explicit ParseResult(ParseResultType type) : type(type) {
	}
	virtual ~ParseResult() = default;

	template <class TARGET>
	TARGET &Cast() {
		if (TARGET::TYPE != ParseResultType::INVALID && type != TARGET::TYPE) {
			throw InternalException("Failed to cast parse result of type %s to type %s for rule %s",
			                        ParseResultToString(TARGET::TYPE), ParseResultToString(type), name);
		}
		return reinterpret_cast<TARGET &>(*this);
	}

	ParseResultType type;
	string name;

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
	string identifier;

	explicit IdentifierParseResult(string identifier_p) : ParseResult(TYPE), identifier(std::move(identifier_p)) {
	}

	void ToStringInternal(std::stringstream &ss, std::unordered_set<const ParseResult *> &visited,
	                      const std::string &indent, bool is_last) const override {
		ParseResult::ToStringInternal(ss, visited, indent, is_last);
		ss << ": " << identifier << "\n";
	}
};

struct KeywordParseResult : ParseResult {
	static constexpr ParseResultType TYPE = ParseResultType::KEYWORD;
	string keyword;

	explicit KeywordParseResult(string keyword_p) : ParseResult(TYPE), keyword(std::move(keyword_p)) {
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
	explicit ListParseResult(vector<optional_ptr<ParseResult>> results_p, string name_p)
	    : ParseResult(TYPE), children(std::move(results_p)) {
		name = name_p;
	}

	vector<optional_ptr<ParseResult>> GetChildren() const {
		return children;
	}

	optional_ptr<ParseResult> GetChild(idx_t index) {
		if (index >= children.size()) {
			throw InternalException("Child index out of bounds");
		}
		return children[index];
	}

	template <class T>
	T &Child(idx_t index) {
		auto child_ptr = GetChild(index);
		return child_ptr->Cast<T>();
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
		ss << " [" << children.size() << " children]\n";

		std::string child_indent = indent + (is_last ? "   " : "│  ");
		for (size_t i = 0; i < children.size(); ++i) {
			if (children[i]) {
				children[i]->ToStringInternal(ss, visited, child_indent, i == children.size() - 1);
			} else {
				ss << child_indent << (i == children.size() - 1 ? "└─" : "├─") << " [nullptr]\n";
			}
		}
	}

private:
	vector<optional_ptr<ParseResult>> children;
};

struct RepeatParseResult : ParseResult {
	static constexpr ParseResultType TYPE = ParseResultType::REPEAT;
	vector<optional_ptr<ParseResult>> children;

	explicit RepeatParseResult(vector<optional_ptr<ParseResult>> results_p)
	    : ParseResult(TYPE), children(std::move(results_p)) {
	}

	template <class T>
	T &Child(idx_t index) {
		if (index >= children.size()) {
			throw InternalException("Child index out of bounds");
		}
		return children[index]->Cast<T>();
	}

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
		ss << " [" << children.size() << " children]\n";

		std::string child_indent = indent + (is_last ? "   " : "│  ");
		for (size_t i = 0; i < children.size(); ++i) {
			if (children[i]) {
				children[i]->ToStringInternal(ss, visited, child_indent, i == children.size() - 1);
			} else {
				ss << child_indent << (i == children.size() - 1 ? "└─" : "├─") << " [nullptr]\n";
			}
		}
	}
};

struct OptionalParseResult : ParseResult {
	static constexpr ParseResultType TYPE = ParseResultType::OPTIONAL;
	optional_ptr<ParseResult> optional_result;

	explicit OptionalParseResult() : ParseResult(TYPE), optional_result(nullptr) {
	}
	explicit OptionalParseResult(optional_ptr<ParseResult> result_p) : ParseResult(TYPE), optional_result(result_p) {
		name = result_p->name;
	}

	bool HasResult() const {
		return optional_result != nullptr;
	}

	void ToStringInternal(std::stringstream &ss, std::unordered_set<const ParseResult *> &visited,
	                      const std::string &indent, bool is_last) const override {
		if (HasResult()) {
			// The optional node has a value, so we "collapse" it by just printing its child.
			// We pass the same indentation and is_last status, so it takes the place of the Optional node.
			optional_result->ToStringInternal(ss, visited, indent, is_last);
		} else {
			// The optional node is empty, which is useful information, so we print it.
			ss << indent << (is_last ? "└─" : "├─") << " " << ParseResultToString(type) << " [empty]\n";
		}
	}
};

class ChoiceParseResult : public ParseResult {
public:
	static constexpr ParseResultType TYPE = ParseResultType::CHOICE;

	explicit ChoiceParseResult(optional_ptr<ParseResult> parse_result_p, idx_t selected_idx_p)
	    : ParseResult(TYPE), result(parse_result_p), selected_idx(selected_idx_p) {
		name = parse_result_p->name;
	}

	optional_ptr<ParseResult> result;
	idx_t selected_idx;

	void ToStringInternal(std::stringstream &ss, std::unordered_set<const ParseResult *> &visited,
	                      const std::string &indent, bool is_last) const override {
		if (result) {
			// The choice was resolved. We print a marker and then print the child below it.
			ss << indent << (is_last ? "└─" : "├─") << " [" << ParseResultToString(type) << " (idx: " << selected_idx
			   << ")] ->\n";

			// The child is now on a new indentation level and is the only child of our marker.
			std::string child_indent = indent + (is_last ? "   " : "│  ");
			result->ToStringInternal(ss, visited, child_indent, true);
		} else {
			// The choice had no result.
			ss << indent << (is_last ? "└─" : "├─") << " " << ParseResultToString(type) << " [no result]\n";
		}
	}
};

class NumberParseResult : public ParseResult {
public:
	static constexpr ParseResultType TYPE = ParseResultType::NUMBER;

	explicit NumberParseResult(string number_p) : ParseResult(TYPE), number(std::move(number_p)) {
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

	explicit StringLiteralParseResult(string string_p) : ParseResult(TYPE), result(std::move(string_p)) {
	}
	string result;

	void ToStringInternal(std::stringstream &ss, std::unordered_set<const ParseResult *> &visited,
	                      const std::string &indent, bool is_last) const override {
		ParseResult::ToStringInternal(ss, visited, indent, is_last);
		ss << ": \"" << result << "\"\n";
	}
};

class OperatorParseResult : public ParseResult {
public:
	static constexpr ParseResultType TYPE = ParseResultType::OPERATOR;

	explicit OperatorParseResult(string operator_p) : ParseResult(TYPE), operator_token(std::move(operator_p)) {
	}
	string operator_token;

	void ToStringInternal(std::stringstream &ss, std::unordered_set<const ParseResult *> &visited,
	                      const std::string &indent, bool is_last) const override {
		ParseResult::ToStringInternal(ss, visited, indent, is_last);
		ss << ": " << operator_token << "\n";
	}
};

} // namespace duckdb
