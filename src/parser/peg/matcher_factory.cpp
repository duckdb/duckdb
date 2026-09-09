#include "duckdb/parser/peg/matcher_factory.hpp"
#include "duckdb/parser/peg/peg_parser.hpp"
#include "duckdb/parser/peg/matcher/list.hpp"
#include "duckdb/parser/peg/compiled_grammar.hpp"

namespace duckdb {

void MatcherFactory::MatcherConstructionState::Register(string_t rule_name) {
	unconstructed.insert(rule_name);
}

void MatcherFactory::MatcherConstructionState::Schedule(string_t rule_name) {
	if (unconstructed.count(rule_name) && scheduled.insert(rule_name).second) {
		pending.push(rule_name);
	}
}

bool MatcherFactory::MatcherConstructionState::Begin(string_t rule_name) {
	return unconstructed.erase(rule_name);
}

bool MatcherFactory::MatcherConstructionState::HasScheduled() const {
	return !pending.empty();
}

string_t MatcherFactory::MatcherConstructionState::TakeNext() {
	auto rule_name = pending.front();
	pending.pop();
	scheduled.erase(rule_name);
	return rule_name;
}

Matcher &MatcherFactory::CreateMatcher(const PEGExpression &expression, const string_map_t<idx_t> &parameter_map,
                                       vector<reference<Matcher>> &parameters) {
	switch (expression.type) {
	case PEGExpression::Type::LITERAL:
		return Keyword(expression.text.GetString());
	case PEGExpression::Type::REFERENCE: {
		auto parameter = parameter_map.find(expression.text);
		if (parameter != parameter_map.end()) {
			return parameters[parameter->second].get();
		}
		auto matcher = matchers.find(expression.text);
		if (matcher != matchers.end()) {
			construction_state.Schedule(expression.text);
			return matcher->second.get();
		}
		return CreateMatcher(expression.text);
	}
	case PEGExpression::Type::FUNCTION_CALL: {
		if (expression.children.size() != 1) {
			throw InternalException("Function call '%s' expected a single argument", expression.text.GetString());
		}
		vector<reference<Matcher>> function_parameters;
		function_parameters.push_back(CreateMatcher(expression.children[0], parameter_map, parameters));
		return CreateMatcher(expression.text, function_parameters);
	}
	case PEGExpression::Type::SEQUENCE: {
		vector<reference<Matcher>> children;
		for (auto &child : expression.children) {
			children.push_back(CreateMatcher(child, parameter_map, parameters));
		}
		return List(std::move(children));
	}
	case PEGExpression::Type::CHOICE: {
		vector<reference<Matcher>> children;
		for (auto &child : expression.children) {
			children.push_back(CreateMatcher(child, parameter_map, parameters));
		}
		return Choice(std::move(children));
	}
	case PEGExpression::Type::OPTIONAL:
	case PEGExpression::Type::REPEAT:
	case PEGExpression::Type::OPTIONAL_REPEAT: {
		if (expression.children.size() != 1) {
			throw InternalException("PEG postfix expression expected a single child");
		}
		auto &child = CreateMatcher(expression.children[0], parameter_map, parameters);
		if (expression.type == PEGExpression::Type::OPTIONAL) {
			return Optional(child);
		}
		auto &repeat = Repeat(child);
		if (expression.type == PEGExpression::Type::OPTIONAL_REPEAT) {
			return Optional(repeat);
		}
		return repeat;
	}
	case PEGExpression::Type::REGEX:
		throw InternalException("REGEX operator not supported in PEG grammar");
	default:
		throw InternalException("Unrecognized PEG expression type");
	}
}

Matcher &MatcherFactory::CreateMatcher(string_t rule_name, vector<reference<Matcher>> &parameters) {
	bool is_function_call = !parameters.empty();
	auto matcher_entry = matchers.find(rule_name);
	if (!is_function_call) {
		if (matcher_entry == matchers.end()) {
			throw InvalidConfigurationException("Recipe references rule %s, which doesn't exist in the grammar",
			                                    rule_name.GetString());
		}
		if (!construction_state.Begin(rule_name)) {
			//! Already constructed, return the cached matcher
			return matcher_entry->second.get();
		}
	} else {
		matcher_entry = matchers.end();
	}
	// Named matchers are registered before any bodies are constructed so recursive references can resolve immediately.
	auto &matcher = is_function_call ? List() : matcher_entry->second.get().Cast<ListMatcher>();

	// fill the matcher from the given set of rules
	// look up the rule
	auto entry = grammar.rules.find(rule_name.GetString());
	if (entry == grammar.rules.end()) {
		throw InvalidConfigurationException("Failed to create matcher for rule %s - rule is missing",
		                                    rule_name.GetString());
	}
	auto &rule = entry->second->recipe;
	if (rule.parameters.size() > 1) {
		throw InvalidConfigurationException("Only functions with a single parameter are supported");
	}
	if (parameters.size() != rule.parameters.size()) {
		throw InvalidConfigurationException("Parameter count mismatch (rule %s expected %d parameters but got %d)",
		                                    rule_name.GetString(), rule.parameters.size(), parameters.size());
	}
	auto &expression_matcher = CreateMatcher(rule.expression, rule.parameters, parameters);
	if (rule.expression.type == PEGExpression::Type::SEQUENCE) {
		matcher.matchers = std::move(expression_matcher.Cast<ListMatcher>().matchers);
	} else {
		matcher.matchers.push_back(expression_matcher);
	}

	auto rule_name_str = rule_name.GetString();
	auto rule_p = compiled.GetRule(rule_name_str);
	if (!rule_p) {
		throw InvalidInputException("Failed to compile rule '%s', no registered data exists for it", rule_name_str);
	}
	auto &compiled_rule = *rule_p;

	matcher.SetRule(compiled_rule);
	if (packrat_memoized_rules.count(rule_name)) {
		matcher.SetPackratMemoized();
	}
	if (no_suggestion_rules.count(rule_name)) {
		matcher.Cast<ListMatcher>().suppress_suggestions = true;
	}
	return matcher;
}

void MatcherFactory::AddKeywordOverride(const char *name, KeywordInfo info) {
	keyword_overrides.insert(make_pair(name, info));
}

void MatcherFactory::AddRuleOverride(const char *name, unique_ptr<Matcher> &&matcher_p) {
	auto &matcher = allocator.Allocate(std::move(matcher_p));
	if (packrat_memoized_rules.count(name)) {
		matcher.SetPackratMemoized();
	}
	if (grammar.GetRule(name)) {
		auto rule_p = compiled.GetRule(name);
		if (!rule_p) {
			throw InvalidInputException("No registered data exists for rule '%s', failed to set RuleOverride", name);
		}
		auto &rule = *rule_p;
		matcher.SetRule(rule);
	}
	matchers.emplace(name, reference<Matcher>(matcher));
}

void MatcherFactory::AddPackratMemoizedRule(const char *name) {
	packrat_memoized_rules.insert(name);
}

void MatcherFactory::SuppressSuggestions(const char *name) {
	no_suggestion_rules.insert(name);
}

MatcherFactory::MatcherFactory(MatcherAllocator &allocator, const ParsedGrammar &grammar_p, CompiledGrammar &compiled_p,
                               terminal_rule_overrides_t terminal_rule_overrides_p)
    : allocator(allocator), grammar(grammar_p), compiled(compiled_p),
      terminal_rule_overrides(std::move(terminal_rule_overrides_p)) {
}

Matcher &MatcherFactory::CreateRootMatcher(const string &root_rule) {
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

	for (auto &entry : terminal_rule_overrides) {
		AddRuleOverride(entry.first.c_str(), std::move(entry.second));
	}

	// suppress suggestions for catch-all rules that would pollute statement-level autocomplete
	SuppressSuggestions("ExpressionStatement");
	// Using SHOW to describe a table/query is deprecated - parse these forms but do not autocomplete them after SHOW
	// (only setting names and the special SHOW forms are offered). DESCRIBE/SUMMARIZE still complete tables and
	// queries.
	SuppressSuggestions("ShowDeprecatedQualifiedTableName");
	SuppressSuggestions("ShowDeprecatedSelect");

	// Register all named rules before constructing any children. Grammar changes can introduce cycles through
	// parameterized rules, so registering only the current recursive path is insufficient.
	for (auto &entry : grammar.rules) {
		if (!entry.second->recipe.parameters.empty()) {
			//! Parameterized rule, can't cache
			continue;
		}
		if (matchers.count(entry.first)) {
			//! Pre-made rule, doesn't get built by the matcher factory
			continue;
		}
		auto &matcher = List();
		auto rule_name = string_t(entry.second->name);
		matchers.emplace(rule_name, reference<Matcher>(matcher));
		construction_state.Register(rule_name);
	}

	// Populate the reachable rules without recursively constructing referenced bodies. The queue grows as references
	// are encountered and therefore also handles cycles that pass through parameterized rules.
	CreateMatcher(root_rule);
	while (construction_state.HasScheduled()) {
		CreateMatcher(construction_state.TakeNext());
	}
	return GetMatcher(root_rule);
}

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

Matcher &MatcherFactory::CreateMatcher(string_t rule_name) {
	vector<reference<Matcher>> parameters;
	return CreateMatcher(rule_name, parameters);
}

} // namespace duckdb
