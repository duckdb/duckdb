#include "duckdb/parser/peg/matcher_factory.hpp"
#include "duckdb/parser/peg/peg_parser.hpp"

namespace duckdb {

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

} // namespace duckdb
