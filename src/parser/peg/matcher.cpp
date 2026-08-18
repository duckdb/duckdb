#include "duckdb/parser/peg/matcher.hpp"
#include "duckdb/parser/peg/matcher/list.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parser/peg/transformer/peg_transformer.hpp"

// uncomment to dynamically read the PEG parser from a file instead of compiling it in (useful for testing)
// #define PEG_PARSER_SOURCE_FILE "duckdb/parser/peg/inlined_grammar.gram"

#include "duckdb/common/printer.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string_map_set.hpp"
#include "duckdb/common/types/string_type.hpp"
#include "duckdb/parser/peg/keyword_helper.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/exception/parser_exception.hpp"
#include "duckdb/parser/peg/tokenizer/tokenizer.hpp"
#include "duckdb/parser/peg/peg_parser.hpp"
#include "duckdb/parser/peg/transformer/parse_result.hpp"
#ifdef PEG_PARSER_SOURCE_FILE
#include <fstream>
#else
#include "duckdb/parser/peg/inlined_grammar.hpp"
#endif

namespace duckdb {

optional_ptr<ParseResult> Matcher::MatchParseResult(MatchState &state) const {
	if (state.packrat_cache && IsPackratMemoized()) {
		return state.packrat_cache->Match(*this, state);
	}
	return MatchParseResultInternal(state);
}

SuggestionType Matcher::AddSuggestion(MatchState &state) const {
	auto entry = state.added_suggestions.find(*this);
	if (entry != state.added_suggestions.end()) {
		return SuggestionType::MANDATORY;
	}
	state.added_suggestions.insert(*this);
	return AddSuggestionInternal(state);
}

string Matcher::GetName() const {
	if (name.empty()) {
		return ToString();
	}
	return name;
}

void Matcher::Print() const {
	Printer::Print(ToString());
}

void MatchState::AddSuggestion(MatcherSuggestion suggestion) {
	suggestions.push_back(std::move(suggestion));
}

Matcher &MatcherAllocator::Allocate(unique_ptr<Matcher> matcher) {
	auto &result = *matcher;
	result.packrat_id = optional_idx(matchers.size());
	matchers.push_back(std::move(matcher));
	return result;
}

optional_ptr<ParseResult> ParseResultAllocator::Allocate(unique_ptr<ParseResult> parse_result) {
	auto result_ptr = parse_result.get();
	parse_results.push_back(std::move(parse_result));
	return optional_ptr<ParseResult>(result_ptr);
}

//! Class for building matchers
class MatcherFactory {
public:
	friend struct MatcherList;

public:
	explicit MatcherFactory(MatcherAllocator &allocator) : allocator(allocator) {
	}
	virtual ~MatcherFactory() = default;

public:
	//! Create a matcher from a PEG grammar
	Matcher &CreateMatcher(const char *grammar, const char *root_rule);
	//! Look up a matcher for a rule that was already built (as a sub-rule of a previous
	//! CreateMatcher call). Throws if the rule has not been built.
	Matcher &GetMatcher(const string &rule_name);

private:
	// Base primitives
	KeywordMatcher &Keyword(const string &keyword) const;
	ListMatcher &List() const;
	ListMatcher &List(vector<reference<Matcher>> matchers) const;
	ChoiceMatcher &Choice(vector<reference<Matcher>> &&matchers) const;
	OptionalMatcher &Optional(Matcher &matcher) const;
	RepeatMatcher &Repeat(Matcher &matcher) const;

	virtual unique_ptr<KeywordMatcher> CreateKeyword(const string &keyword, const KeywordInfo &info) const;
	virtual unique_ptr<ListMatcher> CreateList() const;
	virtual unique_ptr<ChoiceMatcher> CreateChoice(vector<reference<Matcher>> &&matchers) const;
	virtual unique_ptr<OptionalMatcher> CreateOptional(Matcher &matcher) const;
	virtual unique_ptr<RepeatMatcher> CreateRepeat(Matcher &matcher) const;

	void AddKeywordOverride(const char *name, KeywordInfo keyword_info);
	void AddRuleOverride(const char *name, Matcher &matcher);
	void AddPackratMemoizedRule(const char *name);
	void SuppressSuggestions(const char *name);
	Matcher &CreateMatcher(PEGParser &parser, string_t rule_name);
	Matcher &CreateMatcher(PEGParser &parser, string_t rule_name, vector<reference<Matcher>> &parameters);

private:
	MatcherAllocator &allocator;
	string_map_t<reference<Matcher>> matchers;
	mutable case_insensitive_map_t<reference<KeywordMatcher>> keywords;
	case_insensitive_map_t<KeywordInfo> keyword_overrides;
	string_set_t no_suggestion_rules;
	string_set_t packrat_memoized_rules;
};

unique_ptr<KeywordMatcher> MatcherFactory::CreateKeyword(const string &keyword, const KeywordInfo &info) const {
	return make_uniq<KeywordMatcher>(keyword, info);
}

unique_ptr<ListMatcher> MatcherFactory::CreateList() const {
	return make_uniq<ListMatcher>();
}

unique_ptr<ChoiceMatcher> MatcherFactory::CreateChoice(vector<reference<Matcher>> &&matchers) const {
	return make_uniq<ChoiceMatcher>(std::move(matchers));
}

unique_ptr<OptionalMatcher> MatcherFactory::CreateOptional(Matcher &matcher) const {
	return make_uniq<OptionalMatcher>(matcher);
}

unique_ptr<RepeatMatcher> MatcherFactory::CreateRepeat(Matcher &matcher) const {
	return make_uniq<RepeatMatcher>(matcher);
}

KeywordMatcher &MatcherFactory::Keyword(const string &keyword) const {
	auto it = keywords.find(keyword);
	if (it != keywords.end()) {
		return it->second;
	}

	optional<KeywordInfo> info;
	auto entry = keyword_overrides.find(keyword);
	if (entry != keyword_overrides.end()) {
		info.emplace(entry->second);
	} else {
		info.emplace(0, ' ');
	}
	auto &result = allocator.Allocate(CreateKeyword(keyword, *info)).Cast<KeywordMatcher>();
	keywords.emplace(keyword, result);
	return result;
}

ListMatcher &MatcherFactory::List() const {
	return allocator.Allocate(CreateList()).Cast<ListMatcher>();
}

ListMatcher &MatcherFactory::List(vector<reference<Matcher>> matchers) const {
	auto result = CreateList();
	result->matchers = std::move(matchers);
	return allocator.Allocate(std::move(result)).Cast<ListMatcher>();
}

ChoiceMatcher &MatcherFactory::Choice(vector<reference<Matcher>> &&matchers) const {
	return allocator.Allocate(CreateChoice(std::move(matchers))).Cast<ChoiceMatcher>();
}

OptionalMatcher &MatcherFactory::Optional(Matcher &matcher) const {
	return allocator.Allocate(CreateOptional(matcher)).Cast<OptionalMatcher>();
}

RepeatMatcher &MatcherFactory::Repeat(Matcher &matcher) const {
	return allocator.Allocate(CreateRepeat(matcher)).Cast<RepeatMatcher>();
}

Matcher &MatcherFactory::GetMatcher(const string &rule_name) {
	auto entry = matchers.find(rule_name);
	if (entry == matchers.end()) {
		throw InternalException("Matcher for rule '%s' has not been built", rule_name);
	}
	return entry->second.get();
}

Matcher &MatcherFactory::CreateMatcher(PEGParser &parser, string_t rule_name) {
	vector<reference<Matcher>> parameters;
	return CreateMatcher(parser, rule_name, parameters);
}

struct MatcherListEntry {
	explicit MatcherListEntry(Matcher &matcher) : matcher(matcher), function_name(0U) {
	}
	MatcherListEntry(Matcher &matcher, string_t function_name_p) : matcher(matcher), function_name(function_name_p) {
	}

	Matcher &matcher;
	string_t function_name;
};

struct MatcherList {
public:
	explicit MatcherList(PEGParser &parser, MatcherFactory &factory) : parser(parser), factory(factory) {
	}

	void AddMatcher(Matcher &matcher) {
		auto &root_matcher = matchers.back().matcher;
		switch (root_matcher.Type()) {
		case MatcherType::LIST: {
			auto &root_list = root_matcher.Cast<ListMatcher>();
			root_list.matchers.push_back(matcher);
			break;
		}
		case MatcherType::CHOICE:
			// for a choice matcher we need to pop the choice matcher from the stack afterwards
			if (matchers.size() <= 1) {
				throw InternalException("Choice matcher should never be the root in the matcher stack");
			}
			root_matcher.Cast<ChoiceMatcher>().matchers.push_back(matcher);
			if (!matchers.empty()) {
				matchers.pop_back();
			}
			break;
		default:
			throw InternalException("Cannot add matcher to root matcher of this type");
		}
	}
	void AddRootMatcher(Matcher &matcher) {
		matchers.emplace_back(matcher);
	}
	idx_t GetRootMatcherCount() const {
		return matchers.size();
	}
	MatcherListEntry &GetLastRootMatcher() {
		return matchers.back();
	}
	void BeginFunction(string_t function_name) {
		auto &parameter_list = factory.List();
		matchers.emplace_back(parameter_list, function_name);
	}
	void CloseBracket() {
		if (matchers.size() <= 1) {
			throw InternalException("PEG matcher create error - found too many close brackets");
		}
		auto &root_bracket_matcher = matchers.back();
		if (root_bracket_matcher.function_name.GetSize() == 0) {
			// not a function
			auto &bracket_matcher = root_bracket_matcher.matcher;
			// remove the last matcher from the stack
			matchers.pop_back();
			// push it into the last matcher
			AddMatcher(bracket_matcher);
		} else {
			// function matcher
			auto &function_name = root_bracket_matcher.function_name;
			auto &function_parameters = root_bracket_matcher.matcher.Cast<ListMatcher>();

			// wrap the parameters in a list if there is more than one
			auto &parameter = function_parameters.matchers.size() == 1 ? function_parameters.matchers[0].get()
			                                                           : factory.List(function_parameters.matchers);
			vector<reference<Matcher>> parameters;
			parameters.push_back(parameter);
			// do the substitution of the function call
			auto &function_call = factory.CreateMatcher(parser, function_name, parameters);
			// remove the last matcher from the stack
			matchers.pop_back();
			// push it into the last matcher
			AddMatcher(function_call);
		}
	}

private:
	PEGParser &parser;
	MatcherFactory &factory;
	vector<MatcherListEntry> matchers;
};

Matcher &MatcherFactory::CreateMatcher(PEGParser &parser, string_t rule_name, vector<reference<Matcher>> &parameters) {
	bool is_function_call = !parameters.empty();
	if (!is_function_call) {
		// check if the matcher has already been created first
		auto matcher_entry = matchers.find(rule_name);
		if (matcher_entry != matchers.end()) {
			// return the created matcher
			return matcher_entry->second.get();
		}
	}
	// look up the rule
	auto entry = parser.rules.find(rule_name.GetString());
	if (entry == parser.rules.end()) {
		throw InternalException("Failed to create matcher for rule %s - rule is missing", rule_name.GetString());
	}
	// create a matcher and cache it
	// since matchers can be recursive we need to cache it prior to recursively constructing the other rules
	auto &matcher = List();
	if (!is_function_call) {
		matchers.insert(make_pair(rule_name, reference<Matcher>(matcher)));
	}

	MatcherList list(parser, *this);
	list.AddRootMatcher(matcher);
	// fill the matcher from the given set of rules
	auto &rule = entry->second;
	if (rule.parameters.size() > 1) {
		throw InternalException("Only functions with a single parameter are supported");
	}
	if (parameters.size() != rule.parameters.size()) {
		throw InternalException("Parameter count mismatch (rule %s expected %d parameters but got %d)",
		                        rule_name.GetString(), rule.parameters.size(), parameters.size());
	}
	for (idx_t token_idx = 0; token_idx < rule.tokens.size(); token_idx++) {
		auto &token = rule.tokens[token_idx];
		switch (token.type) {
		case PEGTokenType::LITERAL:
			// literal - push the keyword
			list.AddMatcher(Keyword(token.text.GetString()));
			break;
		case PEGTokenType::REFERENCE: {
			// check if we are referring to a keyword
			auto param_entry = rule.parameters.find(token.text);
			if (param_entry != rule.parameters.end()) {
				// refers to a parameter - refer to it directly
				list.AddMatcher(parameters[param_entry->second].get());
			} else {
				// refers to a different rule - create the matcher for that rule
				list.AddMatcher(CreateMatcher(parser, token.text));
			}
			break;
		}
		case PEGTokenType::FUNCTION_CALL: {
			// function call - get the name of the function
			list.BeginFunction(token.text);
			break;
		}
		case PEGTokenType::OPERATOR: {
			// tokens need to be one byte
			auto op_type = token.text.GetData()[0];
			switch (op_type) {
			case '?':
			case '*': {
				// optional/repeat - make the last rule optional/repeat
				auto &last_matcher = list.GetLastRootMatcher().matcher;
				if (last_matcher.Type() != MatcherType::LIST) {
					throw InternalException("Optional/Repeat expected a list matcher");
				}
				auto &list_matcher = last_matcher.Cast<ListMatcher>();
				if (list_matcher.matchers.empty()) {
					throw InternalException("Optional/Repeat rule found as first token");
				}
				auto &final_matcher = list_matcher.matchers.back();
				if (op_type == '*') {
					// * is Optional(Repeat(CHILD))
					final_matcher = Repeat(final_matcher.get());
				}
				auto &replaced_matcher = Optional(final_matcher);
				if (!list_matcher.matchers.empty()) {
					list_matcher.matchers.pop_back();
				}
				list_matcher.matchers.push_back(replaced_matcher);
				break;
			}
			case '+': {
				// Similar to '*' except it's not optional and just repeat (match at least once)
				auto &last_matcher = list.GetLastRootMatcher().matcher;
				if (last_matcher.Type() != MatcherType::LIST) {
					throw InternalException("Repeat expected a list matcher");
				}
				auto &list_matcher = last_matcher.Cast<ListMatcher>();
				if (list_matcher.matchers.empty()) {
					throw InternalException("Repeat rule found as first token");
				}
				auto &final_matcher = list_matcher.matchers.back();
				final_matcher = Repeat(final_matcher.get());
				if (!list_matcher.matchers.empty()) {
					list_matcher.matchers.pop_back();
				}
				list_matcher.matchers.push_back(final_matcher);
				break;
			}
			case '/': {
				// OR operator - this signifies a choice between the last rule and the next rule
				auto &last_root_matcher = list.GetLastRootMatcher().matcher;
				if (last_root_matcher.Type() != MatcherType::LIST) {
					throw InternalException("OR expected a list matcher");
				}
				auto &list_matcher = last_root_matcher.Cast<ListMatcher>();
				if (list_matcher.matchers.empty()) {
					throw InternalException("OR rule found as first token");
				}
				auto &previous_matcher = list_matcher.matchers.back();

				if (previous_matcher.get().Type() == MatcherType::CHOICE) {
					list.AddRootMatcher(previous_matcher);
				} else {
					vector<reference<Matcher>> choice_options;
					choice_options.push_back(previous_matcher);
					auto &new_choice_matcher = Choice(std::move(choice_options));

					if (!list_matcher.matchers.empty()) {
						list_matcher.matchers.pop_back();
					}
					list_matcher.matchers.push_back(new_choice_matcher);

					list.AddRootMatcher(new_choice_matcher);
				}
				break;
			}
			case '(': {
				// bracket open - push a new list matcher onto the stack
				auto &bracket_matcher = List();
				list.AddRootMatcher(bracket_matcher);
				break;
			}
			case ')': {
				list.CloseBracket();
				break;
			}
			case '!': {
				// throw InternalException("NOT operator not supported in PEG grammar (found in rule %s)",
				// rule_name.GetString());
				// FIXME: we just ignore NOT operators here
				break;
			}
			default:
				throw InternalException("unrecognized peg operator type");
			}
			break;
		}
		case PEGTokenType::REGEX:
			throw InternalException("REGEX operator not supported in PEG grammar (found in rule %s)",
			                        rule_name.GetString());
		default:
			throw InternalException("unrecognized peg token type");
		}
	}
	if (list.GetRootMatcherCount() != 1) {
		throw InternalException("PEG matcher create error - unclosed bracket found");
	}
	matcher.SetName(rule_name.GetString());
	if (packrat_memoized_rules.count(rule_name.GetString())) {
		matcher.SetPackratMemoized();
	}
	if (no_suggestion_rules.count(rule_name.GetString())) {
		matcher.Cast<ListMatcher>().suppress_suggestions = true;
	}
	return matcher;
}

void MatcherFactory::AddKeywordOverride(const char *name, KeywordInfo info) {
	keyword_overrides.insert(make_pair(name, info));
}

void MatcherFactory::AddRuleOverride(const char *name, Matcher &matcher) {
	if (packrat_memoized_rules.count(name)) {
		matcher.SetPackratMemoized();
	}
	matchers.insert(make_pair(name, reference<Matcher>(matcher)));
}

void MatcherFactory::AddPackratMemoizedRule(const char *name) {
	packrat_memoized_rules.insert(name);
}

void MatcherFactory::SuppressSuggestions(const char *name) {
	no_suggestion_rules.insert(name);
}

Matcher &MatcherFactory::CreateMatcher(const char *grammar, const char *root_rule) {
	// parse the grammar into a set of rules
	PEGParser parser;
	parser.ParseRules(grammar);

	// keyword overrides
	AddKeywordOverride("TABLE", KeywordInfo(1, ' '));
	AddKeywordOverride(".", KeywordInfo(0, '\0'));
	AddKeywordOverride("(", KeywordInfo(0, '\0'));
	// packrat memoized rules
	//===--------------------------------------------------------------------===//
	// START GENERATED PACKRAT MEMOIZED RULES
	//===--------------------------------------------------------------------===//
	AddPackratMemoizedRule("Expression");
	AddPackratMemoizedRule("LambdaArrowExpression");
	AddPackratMemoizedRule("LogicalOrExpression");
	AddPackratMemoizedRule("LogicalAndExpression");
	AddPackratMemoizedRule("LogicalNotExpression");
	AddPackratMemoizedRule("IsExpression");
	AddPackratMemoizedRule("ComparisonExpression");
	AddPackratMemoizedRule("BitwiseExpression");
	AddPackratMemoizedRule("AdditiveExpression");
	AddPackratMemoizedRule("MultiplicativeExpression");
	AddPackratMemoizedRule("ExponentiationExpression");
	AddPackratMemoizedRule("PrefixExpression");
	AddPackratMemoizedRule("CollateExpression");
	AddPackratMemoizedRule("AtTimeZoneExpression");
	AddPackratMemoizedRule("SingleExpression");
	AddPackratMemoizedRule("BaseExpression");
	AddPackratMemoizedRule("ParensExpression");
	AddPackratMemoizedRule("ParenthesisExpression");
	AddPackratMemoizedRule("Identifier");
	AddPackratMemoizedRule("ColId");
	AddPackratMemoizedRule("ColumnReference");
	AddPackratMemoizedRule("FunctionExpression");
	//===--------------------------------------------------------------------===//
	// END GENERATED PACKRAT MEMOIZED RULES
	//===--------------------------------------------------------------------===//

	// rule overrides
	//===--------------------------------------------------------------------===//
	// START GENERATED RULE OVERRIDES
	//===--------------------------------------------------------------------===//
	AddRuleOverride("Identifier", allocator.Allocate(make_uniq<IdentifierMatcher>(SuggestionState::SUGGEST_VARIABLE)));
	AddRuleOverride("ReservedIdentifier",
	                allocator.Allocate(make_uniq<ReservedIdentifierMatcher>(SuggestionState::SUGGEST_VARIABLE)));
	AddRuleOverride("CatalogName",
	                allocator.Allocate(make_uniq<IdentifierMatcher>(SuggestionState::SUGGEST_CATALOG_NAME)));
	AddRuleOverride("SchemaName",
	                allocator.Allocate(make_uniq<IdentifierMatcher>(SuggestionState::SUGGEST_SCHEMA_NAME)));
	AddRuleOverride("ReservedSchemaName",
	                allocator.Allocate(make_uniq<ReservedIdentifierMatcher>(SuggestionState::SUGGEST_SCHEMA_NAME)));
	AddRuleOverride("TableName", allocator.Allocate(make_uniq<IdentifierMatcher>(SuggestionState::SUGGEST_TABLE_NAME)));
	AddRuleOverride("ReservedTableName",
	                allocator.Allocate(make_uniq<ReservedIdentifierMatcher>(SuggestionState::SUGGEST_TABLE_NAME)));
	AddRuleOverride("ColumnName",
	                allocator.Allocate(make_uniq<IdentifierMatcher>(SuggestionState::SUGGEST_COLUMN_NAME)));
	AddRuleOverride("ReservedColumnName",
	                allocator.Allocate(make_uniq<ReservedIdentifierMatcher>(SuggestionState::SUGGEST_COLUMN_NAME)));
	AddRuleOverride("IndexName", allocator.Allocate(make_uniq<IdentifierMatcher>(SuggestionState::SUGGEST_VARIABLE)));
	AddRuleOverride("ReservedIndexName",
	                allocator.Allocate(make_uniq<ReservedIdentifierMatcher>(SuggestionState::SUGGEST_VARIABLE)));
	AddRuleOverride("SequenceName",
	                allocator.Allocate(make_uniq<IdentifierMatcher>(SuggestionState::SUGGEST_VARIABLE)));
	AddRuleOverride("FunctionName",
	                allocator.Allocate(make_uniq<IdentifierMatcher>(SuggestionState::SUGGEST_SCALAR_FUNCTION_NAME)));
	AddRuleOverride("ReservedFunctionName", allocator.Allocate(make_uniq<ReservedIdentifierMatcher>(
	                                            SuggestionState::SUGGEST_SCALAR_FUNCTION_NAME)));
	AddRuleOverride("ReservedKeyword",
	                allocator.Allocate(make_uniq<ReservedIdentifierMatcher>(SuggestionState::SUGGEST_VARIABLE)));
	AddRuleOverride("TableFunctionName",
	                allocator.Allocate(make_uniq<IdentifierMatcher>(SuggestionState::SUGGEST_TABLE_FUNCTION_NAME)));
	AddRuleOverride("TypeName", allocator.Allocate(make_uniq<IdentifierMatcher>(SuggestionState::SUGGEST_TYPE_NAME)));
	AddRuleOverride("ReservedTypeName",
	                allocator.Allocate(make_uniq<ReservedIdentifierMatcher>(SuggestionState::SUGGEST_TYPE_NAME)));
	AddRuleOverride("PragmaName",
	                allocator.Allocate(make_uniq<IdentifierMatcher>(SuggestionState::SUGGEST_PRAGMA_NAME)));
	AddRuleOverride("SettingName",
	                allocator.Allocate(make_uniq<IdentifierMatcher>(SuggestionState::SUGGEST_SETTING_NAME)));
	AddRuleOverride("CopyOptionName",
	                allocator.Allocate(make_uniq<ReservedIdentifierMatcher>(SuggestionState::SUGGEST_VARIABLE)));
	AddRuleOverride("NumberLiteral", allocator.Allocate(make_uniq<NumberLiteralMatcher>()));
	AddRuleOverride("StringLiteral", allocator.Allocate(make_uniq<StringLiteralMatcher>()));
	AddRuleOverride("OperatorLiteral", allocator.Allocate(make_uniq<OperatorMatcher>()));
	//===--------------------------------------------------------------------===//
	// END GENERATED RULE OVERRIDES
	//===--------------------------------------------------------------------===//

	// EndOfInput has no grammar body; satisfied here (outside the regenerated block).
	AddRuleOverride("EndOfInput", allocator.Allocate(make_uniq<EndOfInputMatcher>()));

	// suppress suggestions for catch-all rules that would pollute statement-level autocomplete
	SuppressSuggestions("ExpressionStatement");

	// now create the matchers for each of the rules recursively - starting at the root rule
	return CreateMatcher(parser, root_rule);
}

shared_ptr<PEGMatcher> PEGMatcher::Get(ClientContext &context) {
	auto &db = DatabaseInstance::GetDatabase(context);
	return PEGMatcher::Get(db);
}

shared_ptr<PEGMatcher> PEGMatcher::Get(DatabaseInstance &db) {
	auto &parser_cache = db.GetParserCache();
	return parser_cache.GetMatcher();
}

shared_ptr<PEGMatcher> ParserCache::GetMatcher() {
	{
		std::unique_lock<std::mutex> lock(mutex);
		if (matcher) {
			return matcher;
		}
	}
	auto new_matcher = make_shared_ptr<PEGMatcher>();
	MatcherFactory factory(new_matcher->allocator);
#ifdef PEG_PARSER_SOURCE_FILE
	std::ifstream t(PEG_PARSER_SOURCE_FILE);
	std::stringstream buffer;
	buffer << t.rdbuf();
	auto grammar_string = buffer.str();

	new_matcher->program_matcher = factory.CreateMatcher(grammar_string.c_str(), "Program");
#else
	new_matcher->program_matcher = factory.CreateMatcher(const_char_ptr_cast(INLINED_PEG_GRAMMAR), "Program");
#endif
	// TopLevelStatement is referenced by Program, so it has already been built and cached.
	new_matcher->top_level_statement_matcher = factory.GetMatcher("TopLevelStatement");
	std::unique_lock<std::mutex> lock(mutex);
	if (!matcher) {
		matcher = std::move(new_matcher);
	}
	return matcher;
}

shared_ptr<PEGTransformerFactory> ParserCache::GetTransformerFactory() {
	{
		std::unique_lock<std::mutex> lock(mutex);
		if (transformer_factory) {
			return transformer_factory;
		}
	}
	auto new_factory = make_shared_ptr<PEGTransformerFactory>();
	std::unique_lock<std::mutex> lock(mutex);
	if (!transformer_factory) {
		transformer_factory = std::move(new_factory);
	}
	return transformer_factory;
}

void ParserCache::Invalidate() {
	std::unique_lock<std::mutex> lock(mutex);
	matcher = nullptr;
	transformer_factory = nullptr;
}

} // namespace duckdb
