#pragma once

#include "duckdb/common/string_map_set.hpp"
#include "duckdb/parser/peg/matcher/list.hpp"

namespace duckdb {
struct PEGParser;

class MatcherFactory;

//! Class for building matchers
class MatcherFactory {
private:
	struct MatcherList {
	public:
		struct Entry {
			explicit Entry(Matcher &matcher) : matcher(matcher), function_name(0U) {
			}
			Entry(Matcher &matcher, string_t function_name_p) : matcher(matcher), function_name(function_name_p) {
			}

			Matcher &matcher;
			string_t function_name;
		};

	public:
		explicit MatcherList(PEGParser &parser, MatcherFactory &factory);
		void AddMatcher(Matcher &matcher);
		void AddRootMatcher(Matcher &matcher);
		idx_t GetRootMatcherCount() const;
		void BeginFunction(string_t function_name);
		void CloseBracket();
		MatcherList::Entry &GetLastRootMatcher();

	private:
		PEGParser &parser;
		MatcherFactory &factory;
		vector<MatcherList::Entry> matchers;
	};

public:
	MatcherFactory(MatcherAllocator &allocator, const PEGKeywordHelper &keyword_helper)
	    : allocator(allocator), keyword_helper(keyword_helper) {
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
	const PEGKeywordHelper &keyword_helper;
	string_map_t<reference<Matcher>> matchers;
	mutable case_insensitive_map_t<reference<KeywordMatcher>> keywords;
	case_insensitive_map_t<KeywordInfo> keyword_overrides;
	string_set_t no_suggestion_rules;
	string_set_t packrat_memoized_rules;
};

} // namespace duckdb
