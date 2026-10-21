#include "duckdb/parser/peg/matcher.hpp"
#include "duckdb/parser/peg/matcher/list.hpp"
#include "duckdb/common/reference_map.hpp"
#include "duckdb/common/bit_utils.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Tail check analysis
//===--------------------------------------------------------------------===//
namespace {

enum class TailBracket : uint8_t { NONE, OPEN, CLOSE };

//! Calls fun(child) for every child of a built-in composite matcher
template <class FUN>
static void ForEachChildMatcher(Matcher &matcher, FUN &&fun) {
	switch (matcher.Type()) {
	case MatcherType::LIST:
		for (auto &child : matcher.Cast<ListMatcher>().matchers) {
			fun(child.get());
		}
		break;
	case MatcherType::CHOICE:
		for (auto &child : matcher.Cast<ChoiceMatcher>().matchers) {
			fun(child.get());
		}
		break;
	case MatcherType::OPTIONAL:
		fun(matcher.Cast<OptionalMatcher>().GetChildMatcher());
		break;
	case MatcherType::REPEAT:
		fun(matcher.Cast<RepeatMatcher>().GetChildMatcher());
		break;
	default:
		break;
	}
}

//! Derived matchers can override StartMatch, so only matchers marked as structural follow the semantics of their type
static bool IsCompositeMatcher(const Matcher &matcher) {
	if (!matcher.IsStructural()) {
		return false;
	}
	auto type = matcher.Type();
	return type == MatcherType::LIST || type == MatcherType::CHOICE || type == MatcherType::OPTIONAL ||
	       type == MatcherType::REPEAT;
}

//! Tokens a matcher consumes outside of balanced bracket pairs; unsafe when they cannot be determined
struct ConsumedTokens {
	MatcherTokenSet tokens;
	bool safe = true;
	//! the tokens include balanced bracket pairs, which the tail scan skips
	bool brackets = false;

	bool Merge(const ConsumedTokens &other) {
		bool changed = tokens.MergeTokens(other.tokens);
		if (safe && !other.safe) {
			safe = false;
			changed = true;
		}
		if (!brackets && other.brackets) {
			brackets = true;
			changed = true;
		}
		return changed;
	}
	bool MarkUnsafe() {
		if (!safe) {
			return false;
		}
		safe = false;
		return true;
	}
};

template <class FUN>
static void ForEachLiteralId(const MatcherTokenSet &set, FUN &&fun) {
	for (idx_t word = 0; word < set.literals.size(); word++) {
		auto bits = set.literals[word];
		while (bits) {
			auto bit = static_cast<idx_t>(CountZeros<uint64_t>::Trailing(bits));
			fun(word * 64 + bit);
			bits &= bits - 1;
		}
	}
}

struct SequenceCheckNode {
	explicit SequenceCheckNode(Matcher &matcher_p) : matcher(matcher_p) {
	}

	Matcher &matcher;
	//! child nodes of a composite matcher, in order
	vector<idx_t> children;
	vector<idx_t> parents;
	ConsumedTokens consumed;
	//! set when the matcher consumes exactly one bracket literal
	TailBracket bracket = TailBracket::NONE;
	idx_t bracket_pair = 0;
	bool queued = false;
};

class SequenceCheckAnalysis {
public:
	SequenceCheckAnalysis(const vector<unique_ptr<Matcher>> &all_matchers, const GrammarLiteralTable &table);

	//! The tail check of every sequence that has one
	vector<pair<reference<ListMatcher>, unique_ptr<MatcherSequenceCheck>>> CreateSequenceChecks();

private:
	static constexpr idx_t BRACKET_PAIRS = 3;
	static constexpr idx_t MAX_LEADING_ALTERNATIVES = 6;
	static constexpr idx_t MAX_FIXED_PREFIX = 6;

	//! Appends the tokens of a matcher that consumes a fixed number of tokens; false once the length can vary
	bool AppendFixedPrefix(idx_t node_index, vector<MatcherTokenSet> &pattern) const;
	//! The check of a sequence, created on first use
	MatcherSequenceCheck &CheckFor(vector<pair<reference<ListMatcher>, unique_ptr<MatcherSequenceCheck>>> &checks,
	                               Matcher &matcher) const;

	TailBracket BracketLiteral(idx_t literal_id, idx_t &pair) const;
	//! Merges the tokens consumed by children[begin, end) outside of the bracket pairs among them into result
	bool AnalyzeSequence(const vector<idx_t> &children, idx_t begin, idx_t end, ConsumedTokens &result) const;
	bool Update(idx_t node_index);
	//! No token can be consumed by the prefix and start the element
	bool Disjoint(const ConsumedTokens &prefix, const MatcherTokenSet &required) const;

private:
	const GrammarLiteralTable &table;
	vector<SequenceCheckNode> nodes;
	uint16_t open_literals[BRACKET_PAIRS];
	uint16_t close_literals[BRACKET_PAIRS];
	//! class of the token spelled like each literal
	vector<uint8_t> literal_classes;
};

SequenceCheckAnalysis::SequenceCheckAnalysis(const vector<unique_ptr<Matcher>> &all_matchers,
                                             const GrammarLiteralTable &table_p)
    : table(table_p) {
	static constexpr const char OPEN_BRACKETS[BRACKET_PAIRS] = {'(', '[', '{'};
	static constexpr const char CLOSE_BRACKETS[BRACKET_PAIRS] = {')', ']', '}'};
	for (idx_t pair = 0; pair < BRACKET_PAIRS; pair++) {
		open_literals[pair] = table.Lookup(&OPEN_BRACKETS[pair], 1).LiteralId();
		close_literals[pair] = table.Lookup(&CLOSE_BRACKETS[pair], 1).LiteralId();
	}
	literal_classes.resize(idx_t(LiteralInfo::MAX_LITERAL_ID) + 1, 0);
	table.ForEachLiteralText([&](const string &text, const LiteralInfo &info) {
		uint8_t token_class;
		if (info.IsKeyword()) {
			token_class = MatcherTokenClass::WORD;
		} else if (!text.empty() && Tokenizer::CharacterIsKeyword(text[0])) {
			token_class = MatcherTokenClass::WORD | MatcherTokenClass::IDENTIFIER;
		} else {
			token_class = ComputeMatcherTokenClass(TokenType::OPERATOR, text);
		}
		literal_classes[info.LiteralId()] = token_class;
	});
	// only reachable matchers have FIRST sets
	reference_map_t<const Matcher, idx_t> index;
	idx_t literal_words = 0;
	for (auto &matcher_p : all_matchers) {
		auto &first = matcher_p->first_set;
		if (!first.computed) {
			continue;
		}
		index[*matcher_p] = nodes.size();
		nodes.emplace_back(*matcher_p);
		literal_words = MaxValue(literal_words, MaxValue(first.literals.size(), first.second.literals.size()));
	}
	for (idx_t node_index = 0; node_index < nodes.size(); node_index++) {
		auto &node = nodes[node_index];
		auto &matcher = node.matcher;
		auto &first = matcher.first_set;
		if (IsCompositeMatcher(matcher)) {
			bool complete = true;
			ForEachChildMatcher(matcher, [&](Matcher &child) {
				auto entry = index.find(child);
				if (entry == index.end()) {
					complete = false;
				} else {
					node.children.push_back(entry->second);
				}
			});
			if (!complete) {
				node.children.clear();
				node.consumed.safe = false;
			}
			for (auto child : node.children) {
				nodes[child].parents.push_back(node_index);
			}
			// sized once, so merging never reallocates
			node.consumed.tokens.literals.resize(literal_words, 0);
		} else if (matcher.IsAtomic() && !first.any && !first.one.any && !first.second.any) {
			// built-in atomic matchers consume tokens of their FIRST and SECOND sets only
			node.consumed.tokens.MergeTokens(first);
			node.consumed.tokens.MergeTokens(first.second);
			if (!first.class_mask && first.second.IsEmpty()) {
				idx_t literal_count = 0;
				idx_t only_literal = 0;
				ForEachLiteralId(first, [&](idx_t literal_id) {
					literal_count++;
					only_literal = literal_id;
				});
				if (literal_count == 1) {
					node.bracket = BracketLiteral(only_literal, node.bracket_pair);
				}
			}
			// a bracket is only consumed as part of a balanced pair in one sequence
			if (node.bracket != TailBracket::NONE) {
				node.consumed.safe = false;
			}
		} else {
			node.consumed.safe = false;
		}
	}
}

TailBracket SequenceCheckAnalysis::BracketLiteral(idx_t literal_id, idx_t &pair) const {
	for (pair = 0; pair < BRACKET_PAIRS; pair++) {
		if (literal_id == open_literals[pair]) {
			return TailBracket::OPEN;
		}
		if (literal_id == close_literals[pair]) {
			return TailBracket::CLOSE;
		}
	}
	return TailBracket::NONE;
}

bool SequenceCheckAnalysis::AnalyzeSequence(const vector<idx_t> &children, idx_t begin, idx_t end,
                                            ConsumedTokens &result) const {
	bool changed = false;
	idx_t i = begin;
	while (i < end) {
		auto &child = nodes[children[i]];
		if (child.bracket == TailBracket::NONE) {
			changed |= result.Merge(child.consumed);
			i++;
			continue;
		}
		if (child.bracket == TailBracket::CLOSE) {
			return changed | result.MarkUnsafe();
		}
		// find the closing bracket of the same pair, nested pairs included
		idx_t depth = 0;
		idx_t close = i + 1;
		for (; close < end; close++) {
			auto &candidate = nodes[children[close]];
			if (candidate.bracket == TailBracket::NONE || candidate.bracket_pair != child.bracket_pair) {
				continue;
			}
			if (candidate.bracket == TailBracket::OPEN) {
				depth++;
			} else if (depth == 0) {
				break;
			} else {
				depth--;
			}
		}
		if (close >= end) {
			return changed | result.MarkUnsafe();
		}
		// the interior is skipped at runtime, but it has to keep its own brackets balanced
		for (idx_t interior = i + 1; interior < close; interior++) {
			auto &interior_node = nodes[children[interior]];
			if (interior_node.bracket == TailBracket::NONE && !interior_node.consumed.safe) {
				changed |= result.MarkUnsafe();
			}
		}
		if (!result.brackets) {
			result.brackets = true;
			changed = true;
		}
		i = close + 1;
	}
	return changed;
}

bool SequenceCheckAnalysis::AppendFixedPrefix(idx_t node_index, vector<MatcherTokenSet> &pattern) const {
	auto &node = nodes[node_index];
	auto &matcher = node.matcher;
	auto &first = matcher.first_set;
	if (pattern.size() >= MAX_FIXED_PREFIX || !first.computed || first.any || first.nullable) {
		return false;
	}
	if (matcher.IsAtomic()) {
		// single-token atomics: FIRST is exactly the one token they consume
		if (first.one.any || first.second.any || !first.second.IsEmpty()) {
			return false;
		}
		pattern.emplace_back(static_cast<const MatcherTokenSet &>(first));
		return true;
	}
	if (!IsCompositeMatcher(matcher)) {
		return false;
	}
	switch (matcher.Type()) {
	case MatcherType::LIST:
		for (auto child : node.children) {
			if (!AppendFixedPrefix(child, pattern)) {
				return false;
			}
		}
		return !node.children.empty();
	case MatcherType::CHOICE: {
		// alternatives that are all single tokens: any of them
		MatcherTokenSet tokens;
		for (auto child : node.children) {
			auto &alternative = nodes[child].matcher;
			auto &alternative_first = alternative.first_set;
			if (!alternative.IsAtomic() || alternative_first.any || alternative_first.one.any ||
			    alternative_first.second.any || !alternative_first.second.IsEmpty()) {
				return false;
			}
			tokens.MergeTokens(alternative_first);
		}
		pattern.push_back(std::move(tokens));
		return !node.children.empty();
	}
	default:
		return false;
	}
}

MatcherSequenceCheck &
SequenceCheckAnalysis::CheckFor(vector<pair<reference<ListMatcher>, unique_ptr<MatcherSequenceCheck>>> &checks,
                                Matcher &matcher) const {
	for (auto &entry : checks) {
		if (&entry.first.get() == &matcher) {
			return *entry.second;
		}
	}
	checks.emplace_back(matcher.Cast<ListMatcher>(), make_uniq<MatcherSequenceCheck>());
	return *checks.back().second;
}

bool SequenceCheckAnalysis::Update(idx_t node_index) {
	auto &node = nodes[node_index];
	if (node.matcher.Type() == MatcherType::LIST) {
		return AnalyzeSequence(node.children, 0, node.children.size(), node.consumed);
	}
	bool changed = false;
	for (auto child : node.children) {
		changed |= node.consumed.Merge(nodes[child].consumed);
	}
	return changed;
}

bool SequenceCheckAnalysis::Disjoint(const ConsumedTokens &prefix, const MatcherTokenSet &required) const {
	auto &alphabet = prefix.tokens;
	// the classes the tokenizer assigns, so that a class in one set cannot share a token with a class in the other
	static constexpr uint8_t TOKEN_CLASSES[] = {
	    MatcherTokenClass::WORD,     MatcherTokenClass::WORD | MatcherTokenClass::IDENTIFIER,
	    MatcherTokenClass::STRING,   MatcherTokenClass::NUMBER,
	    MatcherTokenClass::OPERATOR, MatcherTokenClass::OPERATOR | MatcherTokenClass::GENERIC_OPERATOR};
	for (auto token_class : TOKEN_CLASSES) {
		if ((token_class & alphabet.class_mask) && (token_class & required.class_mask)) {
			return false;
		}
	}
	bool disjoint = true;
	ForEachLiteralId(required, [&](idx_t literal_id) {
		idx_t pair;
		auto bracket = BracketLiteral(literal_id, pair);
		// the scan skips bracket pairs the prefix consumes, so a required bracket is only found when it has none
		if (alphabet.HasLiteral(literal_id) || (literal_classes[literal_id] & alphabet.class_mask) ||
		    bracket == TailBracket::CLOSE || (bracket == TailBracket::OPEN && prefix.brackets)) {
			disjoint = false;
		}
	});
	ForEachLiteralId(alphabet, [&](idx_t literal_id) {
		if (literal_classes[literal_id] & required.class_mask) {
			disjoint = false;
		}
	});
	return disjoint;
}

vector<pair<reference<ListMatcher>, unique_ptr<MatcherSequenceCheck>>> SequenceCheckAnalysis::CreateSequenceChecks() {
	// least fixed point over the recursive grammar: a change re-queues the parents
	vector<idx_t> worklist;
	for (idx_t node_index = 0; node_index < nodes.size(); node_index++) {
		if (IsCompositeMatcher(nodes[node_index].matcher)) {
			nodes[node_index].queued = true;
			worklist.push_back(node_index);
		}
	}
	while (!worklist.empty()) {
		auto node_index = worklist.back();
		worklist.pop_back();
		nodes[node_index].queued = false;
		if (!Update(node_index)) {
			continue;
		}
		for (auto parent : nodes[node_index].parents) {
			if (!nodes[parent].queued) {
				nodes[parent].queued = true;
				worklist.push_back(parent);
			}
		}
	}
	vector<pair<reference<ListMatcher>, unique_ptr<MatcherSequenceCheck>>> sequence_checks;
	for (auto &node : nodes) {
		auto &matcher = node.matcher;
		if (!IsCompositeMatcher(matcher) || matcher.Type() != MatcherType::LIST) {
			continue;
		}
		auto &children = node.children;
		for (idx_t i = 1; i < children.size(); i++) {
			auto &element_first = nodes[children[i]].matcher.first_set;
			if (!element_first.computed || element_first.any || element_first.nullable) {
				continue;
			}
			// a one-token prefix is already covered by the SECOND set
			if (i == 1 && nodes[children[0]].matcher.IsAtomic()) {
				continue;
			}
			ConsumedTokens prefix;
			AnalyzeSequence(children, 0, i, prefix);
			if (!prefix.safe || !Disjoint(prefix, element_first)) {
				continue;
			}
			auto check = make_uniq<MatcherSequenceCheck>();
			check->has_tail = true;
			check->bracket_ends_prefix = !prefix.brackets;
			check->alphabet = std::move(prefix.tokens);
			check->required = static_cast<const MatcherTokenSet &>(element_first);
			sequence_checks.emplace_back(matcher.Cast<ListMatcher>(), std::move(check));
			break;
		}
	}
	// a leading choice with few alternatives is checked per alternative
	for (auto &node : nodes) {
		auto &matcher = node.matcher;
		if (!IsCompositeMatcher(matcher) || matcher.Type() != MatcherType::LIST || node.children.empty()) {
			continue;
		}
		// rules wrap their body in a one-element sequence
		auto leading_index = node.children[0];
		while (true) {
			auto &leading_node = nodes[leading_index];
			if (IsCompositeMatcher(leading_node.matcher) && leading_node.matcher.Type() == MatcherType::LIST &&
			    leading_node.children.size() == 1) {
				leading_index = leading_node.children[0];
				continue;
			}
			break;
		}
		auto &leading = nodes[leading_index].matcher;
		if (!IsCompositeMatcher(leading) || leading.Type() != MatcherType::CHOICE) {
			continue;
		}
		auto &choice = leading.Cast<ChoiceMatcher>();
		if (choice.matchers.size() < 2 || choice.matchers.size() > MAX_LEADING_ALTERNATIVES) {
			continue;
		}
		bool informative = false;
		for (auto &alternative : choice.matchers) {
			auto &first = alternative.get().first_set;
			if (!first.computed || first.any || first.nullable) {
				informative = false;
				break;
			}
			// an alternative whose second token is constrained can reject what the merged sets accept
			if (!first.first_suffices) {
				informative = true;
			}
		}
		if (!informative) {
			continue;
		}
		auto &check = CheckFor(sequence_checks, matcher);
		check.leading_choice = choice;
		check.rest_nullable = true;
		for (idx_t i = 1; i < node.children.size() && check.rest_nullable; i++) {
			auto &rest_first = nodes[node.children[i]].matcher.first_set;
			check.rest_first.MergeTokens(rest_first);
			check.rest_nullable = rest_first.nullable;
		}
	}
	// leading single-token elements fix the tokens beyond the two the FIRST and SECOND sets cover
	for (idx_t node_index = 0; node_index < nodes.size(); node_index++) {
		auto &node = nodes[node_index];
		auto &matcher = node.matcher;
		if (!IsCompositeMatcher(matcher) || matcher.Type() != MatcherType::LIST || node.children.empty()) {
			continue;
		}
		vector<MatcherTokenSet> pattern;
		AppendFixedPrefix(node_index, pattern);
		if (pattern.size() < 3) {
			continue;
		}
		auto &check = CheckFor(sequence_checks, matcher);
		check.later_tokens.assign(pattern.begin() + 2, pattern.end());
	}
	return sequence_checks;
}

} // namespace

void MatcherAllocator::InitializeSequenceChecks(const GrammarLiteralTable &table) {
	// Clear borrowed links before releasing their owners or making any allocation that can throw.
	for (auto &matcher : matchers) {
		auto &first = matcher->first_set;
		if (first.sequence_check_index) {
			first.sequence_check_index = 0;
			first.Finalize();
		}
	}
	sequence_checks.clear();
	SequenceCheckAnalysis analysis(matchers, table);
	auto checks = analysis.CreateSequenceChecks();
	sequence_checks.reserve(checks.size());
	for (auto &entry : checks) {
		if (sequence_checks.size() >= NumericLimits<uint16_t>::Maximum()) {
			break;
		}
		auto &first = entry.first.get().first_set;
		sequence_checks.push_back(std::move(entry.second));
		first.sequence_check_index = NumericCast<uint16_t>(sequence_checks.size());
		// every token passing the FIRST check goes through MightMatchSecond, which runs the checks
		first.first_suffices = false;
	}
	// a repetition of a checked sequence starts like the sequence
	for (auto &matcher : matchers) {
		if (!matcher->IsStructural() || matcher->Type() != MatcherType::REPEAT) {
			continue;
		}
		auto &child = matcher->Cast<RepeatMatcher>().GetChildMatcher();
		if (!child.first_set.sequence_check_index || child.first_set.nullable) {
			continue;
		}
		matcher->first_set.sequence_check_index = child.first_set.sequence_check_index;
		matcher->first_set.first_suffices = false;
	}
}

} // namespace duckdb
