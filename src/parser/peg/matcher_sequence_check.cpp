#include "duckdb/parser/peg/matcher.hpp"
#include "duckdb/parser/peg/matcher/list.hpp"
#include "duckdb/common/reference_map.hpp"
#include "duckdb/common/bit_utils.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Sequence check
//===--------------------------------------------------------------------===//
namespace {

//! How many tokens the tail scan looks at before it accepts
constexpr idx_t MAX_TAIL_SCAN = 128;

enum class BracketKind { NONE, OPEN, CLOSE };

struct BracketPair {
	char open;
	char close;
};

//! The bracket pairs whose content the tail scan skips
constexpr BracketPair BRACKET_PAIRS[] = {{'(', ')'}, {'[', ']'}, {'{', '}'}};

BracketKind TokenBracket(const MatcherToken &token) {
	if (token.length != 1) {
		return BracketKind::NONE;
	}
	for (auto &bracket_pair : BRACKET_PAIRS) {
		if (token.text[0] == bracket_pair.open) {
			return BracketKind::OPEN;
		}
		if (token.text[0] == bracket_pair.close) {
			return BracketKind::CLOSE;
		}
	}
	return BracketKind::NONE;
}

//! String literals also carry the WORD class for name positions; alphabets mark string consumption with STRING
uint8_t TailTokenClass(const MatcherToken &token) {
	return token.type == TokenType::STRING_LITERAL ? MatcherTokenClass::STRING : token.token_class;
}

//! A skipped sequence could have come as far as the token that stopped the scan
bool RejectTail(MatchState &state, idx_t stop_position) {
	if (stop_position > state.context.tail_rejected_until) {
		state.context.tail_rejected_until = stop_position;
	}
	return false;
}

} // namespace

bool MatcherSequenceCheck::MightMatch(MatchState &state) const {
	auto &iterator = state.token_iterator;
	for (idx_t i = 0; i < later_tokens.size(); i++) {
		auto ahead = i + 2;
		auto later = iterator.Peek(ahead);
		// the autocomplete cursor can stand for any token
		if (!later || later->type == TokenType::END_OF_INPUT_AUTOCOMPLETE) {
			return true;
		}
		if (!later_tokens[i].Contains(*later, [&]() { return iterator.PeekLiteralInfo(table, ahead); })) {
			return RejectTail(state, iterator.Position() + ahead);
		}
	}
	if (leading_choice && !MightMatchLeadingChoice(state)) {
		return false;
	}
	return !has_tail || MightMatchTail(state);
}

bool MatcherSequenceCheck::MightMatchLeadingChoice(MatchState &state) const {
	auto &iterator = state.token_iterator;
	auto token = iterator.Current();
	auto next = iterator.Peek(1);
	// the autocomplete cursor can stand for any token
	if (!next || next->type == TokenType::END_OF_INPUT_AUTOCOMPLETE) {
		return true;
	}
	auto current_literal = [&]() {
		return iterator.CurrentLiteralInfo(table);
	};
	auto next_literal = [&]() {
		return iterator.PeekLiteralInfo(table, 1);
	};
	auto &alternatives = leading_choice->matchers;
	for (idx_t i = 0; i < alternatives.size(); i++) {
		auto &first = alternatives[i].get().first_set;
		if (!first.computed || first.any || first.nullable) {
			return true;
		}
		if (!first.Contains(*token, current_literal)) {
			continue;
		}
		// an alternative that is a checked sequence itself cannot match when its own check rejects it
		auto alternative_check = alternative_checks[i];
		if (alternative_check && !alternative_check->MightMatch(state)) {
			continue;
		}
		if (first.second.Contains(*next, next_literal)) {
			return true;
		}
		// a one-token alternative is followed by the rest of the sequence
		if (first.one.Contains(*token, current_literal) &&
		    (rest_nullable || rest_first.Contains(*next, next_literal))) {
			return true;
		}
	}
	// the sequence would have consumed the current token before failing: keep the error position
	if (iterator.Position() + 1 > state.context.max_token_index) {
		state.context.max_token_index = iterator.Position() + 1;
	}
	return false;
}

bool MatcherSequenceCheck::MightMatchTail(MatchState &state) const {
	auto &iterator = state.token_iterator;
	idx_t depth = 0;
	for (idx_t ahead = 0; ahead < MAX_TAIL_SCAN; ahead++) {
		auto token = iterator.Peek(ahead);
		// the autocomplete cursor can stand for any token
		if (!token || token->type == TokenType::END_OF_INPUT_AUTOCOMPLETE) {
			return true;
		}
		switch (TokenBracket(*token)) {
		case BracketKind::OPEN:
			if (depth == 0 && bracket_ends_prefix) {
				// the prefix consumes no brackets: this one has to start the element
				auto literal_id = iterator.PeekLiteralInfo(table, ahead).LiteralId();
				if (literal_id && required.HasLiteral(literal_id)) {
					return true;
				}
				return RejectTail(state, iterator.Position() + ahead);
			}
			depth++;
			continue;
		case BracketKind::CLOSE:
			if (depth == 0) {
				// neither the prefix nor the element can consume an unbalanced closing bracket
				return RejectTail(state, iterator.Position() + ahead);
			}
			depth--;
			continue;
		default:
			break;
		}
		if (depth > 0) {
			continue;
		}
		auto token_class = TailTokenClass(*token);
		if (token_class & alphabet.class_mask) {
			continue;
		}
		auto literal_id = iterator.PeekLiteralInfo(table, ahead).LiteralId();
		if (literal_id && alphabet.HasLiteral(literal_id)) {
			continue;
		}
		// the first token the prefix cannot consume has to start the element
		if (token_class & required.class_mask) {
			return true;
		}
		if (literal_id && required.HasLiteral(literal_id)) {
			return true;
		}
		return RejectTail(state, iterator.Position() + ahead);
	}
	return true;
}

//===--------------------------------------------------------------------===//
// Sequence check analysis
//===--------------------------------------------------------------------===//
namespace {

//! Calls fun(child) for every child of a built-in composite matcher
template <class FUN>
void ForEachChildMatcher(Matcher &matcher, FUN &&fun) {
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
bool IsCompositeMatcher(const Matcher &matcher) {
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
	//! every bracket the matcher consumes belongs to a balanced pair it consumes as well
	bool balanced = true;
	//! the tokens include balanced bracket pairs, which the tail scan skips
	bool brackets = false;

	bool Merge(const ConsumedTokens &other) {
		bool changed = tokens.MergeTokens(other.tokens);
		if (safe && !other.safe) {
			safe = false;
			changed = true;
		}
		if (balanced && !other.balanced) {
			balanced = false;
			changed = true;
		}
		if (!brackets && other.brackets) {
			brackets = true;
			changed = true;
		}
		return changed;
	}
	bool MarkUnbalanced() {
		if (!safe && !balanced) {
			return false;
		}
		safe = false;
		balanced = false;
		return true;
	}
};

template <class FUN>
void ForEachLiteralId(const MatcherTokenSet &set, FUN &&fun) {
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
	BracketKind bracket = BracketKind::NONE;
	idx_t bracket_pair = 0;
	bool queued = false;
	//! the check of a sequence, when the analysis derives one
	unique_ptr<MatcherSequenceCheck> check;
};

//! A sequence and the check derived for it
struct CheckedSequence {
	ListMatcher &sequence;
	unique_ptr<MatcherSequenceCheck> check;
};

class SequenceCheckAnalysis {
public:
	SequenceCheckAnalysis(const vector<unique_ptr<Matcher>> &all_matchers, const GrammarLiteralTable &table);

public:
	//! The check of every sequence that has one
	vector<CheckedSequence> CreateSequenceChecks();

private:
	//! Appends the tokens of a matcher that consumes a fixed number of tokens; false once the length can vary
	bool AppendFixedPrefix(idx_t node_index, vector<MatcherTokenSet> &pattern) const;
	//! The check of a sequence, created on first use
	MatcherSequenceCheck &CheckFor(SequenceCheckNode &node) const;

	//! What a matcher that is not a built-in composite consumes
	void InitializeLeaf(SequenceCheckNode &node) const;
	//! Whether a literal is a bracket, and of which pair of BRACKET_PAIRS
	BracketKind BracketLiteral(idx_t literal_id, idx_t &pair_idx) const;
	//! The bracket a matcher consumes when it matches exactly one literal, a bracket
	BracketKind SingleBracket(const MatcherFirstSet &first, idx_t &pair_idx) const;
	//! Merges the tokens consumed by children[begin, end) outside of the bracket pairs among them into result
	bool AnalyzeSequence(const vector<idx_t> &children, idx_t begin, idx_t end, ConsumedTokens &result) const;
	bool Update(idx_t node_index);
	//! No token can be consumed by the prefix and start the element
	bool Disjoint(const ConsumedTokens &prefix, const MatcherTokenSet &required) const;

private:
	//! A leading choice with more alternatives is not checked per alternative
	static constexpr idx_t MAX_LEADING_ALTERNATIVES = 6;
	//! The number of leading tokens of a sequence that are recorded
	static constexpr idx_t MAX_FIXED_PREFIX = 6;

	const GrammarLiteralTable &table;
	vector<SequenceCheckNode> nodes;
	//! literal ids of the brackets of each pair of BRACKET_PAIRS
	vector<uint16_t> open_literals;
	vector<uint16_t> close_literals;
	//! class of the token spelled like each literal
	vector<uint8_t> literal_classes;
};

SequenceCheckAnalysis::SequenceCheckAnalysis(const vector<unique_ptr<Matcher>> &all_matchers,
                                             const GrammarLiteralTable &table_p)
    : table(table_p) {
	for (auto &bracket_pair : BRACKET_PAIRS) {
		open_literals.push_back(table.Lookup(&bracket_pair.open, 1).LiteralId());
		close_literals.push_back(table.Lookup(&bracket_pair.close, 1).LiteralId());
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
				node.consumed.MarkUnbalanced();
			}
			for (auto child : node.children) {
				nodes[child].parents.push_back(node_index);
			}
			// sized once, so merging never reallocates
			node.consumed.tokens.literals.resize(literal_words, 0);
		} else {
			InitializeLeaf(node);
		}
	}
}

void SequenceCheckAnalysis::InitializeLeaf(SequenceCheckNode &node) const {
	auto &matcher = node.matcher;
	if (!matcher.IsAtomic()) {
		// a matcher with a match process of its own can consume anything
		node.consumed.MarkUnbalanced();
		return;
	}
	auto &first = matcher.first_set;
	const bool tokens_known = !first.any && !first.one.any && !first.second.any;
	if (!tokens_known) {
		if (static_cast<const AtomicMatcher &>(matcher).ConsumesBrackets()) {
			node.consumed.MarkUnbalanced();
		} else {
			// the tokens are unknown, but a sequence that brackets this matcher stays balanced
			node.consumed.safe = false;
		}
		return;
	}
	// built-in atomic matchers consume tokens of their FIRST and SECOND sets only
	node.consumed.tokens.MergeTokens(first);
	node.consumed.tokens.MergeTokens(first.second);
	node.bracket = SingleBracket(first, node.bracket_pair);
	if (node.bracket != BracketKind::NONE) {
		// a bracket is only consumed as part of a balanced pair in one sequence
		node.consumed.MarkUnbalanced();
	}
}

BracketKind SequenceCheckAnalysis::SingleBracket(const MatcherFirstSet &first, idx_t &pair_idx) const {
	if (first.class_mask || !first.second.IsEmpty()) {
		return BracketKind::NONE;
	}
	idx_t literal_count = 0;
	idx_t only_literal = 0;
	ForEachLiteralId(first, [&](idx_t literal_id) {
		literal_count++;
		only_literal = literal_id;
	});
	return literal_count == 1 ? BracketLiteral(only_literal, pair_idx) : BracketKind::NONE;
}

BracketKind SequenceCheckAnalysis::BracketLiteral(idx_t literal_id, idx_t &pair_idx) const {
	for (pair_idx = 0; pair_idx < open_literals.size(); pair_idx++) {
		if (literal_id == open_literals[pair_idx]) {
			return BracketKind::OPEN;
		}
		if (literal_id == close_literals[pair_idx]) {
			return BracketKind::CLOSE;
		}
	}
	return BracketKind::NONE;
}

bool SequenceCheckAnalysis::AnalyzeSequence(const vector<idx_t> &children, idx_t begin, idx_t end,
                                            ConsumedTokens &result) const {
	bool changed = false;
	idx_t child_idx = begin;
	while (child_idx < end) {
		auto &child = nodes[children[child_idx]];
		if (child.bracket == BracketKind::NONE) {
			changed |= result.Merge(child.consumed);
			child_idx++;
			continue;
		}
		if (child.bracket == BracketKind::CLOSE) {
			return changed | result.MarkUnbalanced();
		}
		// find the closing bracket of the same pair, nested pairs included
		idx_t depth = 0;
		idx_t close = child_idx + 1;
		for (; close < end; close++) {
			auto &candidate = nodes[children[close]];
			if (candidate.bracket == BracketKind::NONE || candidate.bracket_pair != child.bracket_pair) {
				continue;
			}
			if (candidate.bracket == BracketKind::OPEN) {
				depth++;
			} else if (depth == 0) {
				break;
			} else {
				depth--;
			}
		}
		if (close >= end) {
			return changed | result.MarkUnbalanced();
		}
		// the interior is skipped at runtime, so only its brackets matter: they have to be balanced
		for (idx_t interior = child_idx + 1; interior < close; interior++) {
			auto &interior_node = nodes[children[interior]];
			if (interior_node.bracket == BracketKind::NONE && !interior_node.consumed.balanced) {
				changed |= result.MarkUnbalanced();
			}
		}
		if (!result.brackets) {
			result.brackets = true;
			changed = true;
		}
		child_idx = close + 1;
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

MatcherSequenceCheck &SequenceCheckAnalysis::CheckFor(SequenceCheckNode &node) const {
	if (!node.check) {
		node.check = make_uniq<MatcherSequenceCheck>(table);
	}
	return *node.check;
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
	// A token's class can have two bits (an identifier is WORD | IDENTIFIER), so two masks without a common bit can
	// still accept the same token. Test every class the tokenizer assigns (ComputeMatcherTokenClass) instead.
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
		idx_t pair_idx;
		auto bracket = BracketLiteral(literal_id, pair_idx);
		// the scan skips bracket pairs the prefix consumes, so a required bracket is only found when it has none
		if (alphabet.HasLiteral(literal_id) || (literal_classes[literal_id] & alphabet.class_mask) ||
		    bracket == BracketKind::CLOSE || (bracket == BracketKind::OPEN && prefix.brackets)) {
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

vector<CheckedSequence> SequenceCheckAnalysis::CreateSequenceChecks() {
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
	// a tail check for the first element that cannot start with a token its prefix consumes
	for (auto &node : nodes) {
		auto &matcher = node.matcher;
		if (!IsCompositeMatcher(matcher) || matcher.Type() != MatcherType::LIST) {
			continue;
		}
		auto &children = node.children;
		for (idx_t element_idx = 1; element_idx < children.size(); element_idx++) {
			auto &element_first = nodes[children[element_idx]].matcher.first_set;
			if (!element_first.computed || element_first.any || element_first.nullable) {
				continue;
			}
			// a one-token prefix is already covered by the SECOND set
			if (element_idx == 1 && nodes[children[0]].matcher.IsAtomic()) {
				continue;
			}
			ConsumedTokens prefix;
			AnalyzeSequence(children, 0, element_idx, prefix);
			if (!prefix.safe || !Disjoint(prefix, element_first)) {
				continue;
			}
			// the element has to start with a token of its FIRST set
			const MatcherTokenSet &required = element_first;
			CheckFor(node).SetTail(std::move(prefix.tokens), required, !prefix.brackets);
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
		// what can follow a one-token alternative: the FIRST set of the rest of the sequence
		MatcherTokenSet rest_first;
		bool rest_nullable = true;
		for (idx_t element_idx = 1; element_idx < node.children.size() && rest_nullable; element_idx++) {
			auto &element_first = nodes[node.children[element_idx]].matcher.first_set;
			rest_first.MergeTokens(element_first);
			rest_nullable = element_first.nullable;
		}
		CheckFor(node).SetLeadingChoice(choice, std::move(rest_first), rest_nullable);
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
		CheckFor(node).SetLaterTokens(vector<MatcherTokenSet>(pattern.begin() + 2, pattern.end()));
	}
	vector<CheckedSequence> sequence_checks;
	for (auto &node : nodes) {
		if (node.check) {
			sequence_checks.push_back({node.matcher.Cast<ListMatcher>(), std::move(node.check)});
		}
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
		auto &first = entry.sequence.first_set;
		sequence_checks.push_back(std::move(entry.check));
		first.sequence_check_index = NumericCast<uint16_t>(sequence_checks.size());
		// every token passing the FIRST check goes through MightMatchSecond, which runs the checks
		first.first_suffices = false;
	}
	// the alternatives of a leading choice that are checked sequences themselves, behind their rule wrappers
	for (auto &check : sequence_checks) {
		auto leading_choice = check->GetLeadingChoice();
		if (!leading_choice) {
			continue;
		}
		vector<optional_ptr<const MatcherSequenceCheck>> alternative_checks;
		for (auto &alternative : leading_choice->matchers) {
			reference<const Matcher> sequence(alternative.get());
			while (!sequence.get().first_set.sequence_check_index && sequence.get().IsStructural() &&
			       sequence.get().Type() == MatcherType::LIST &&
			       sequence.get().Cast<ListMatcher>().matchers.size() == 1) {
				sequence = sequence.get().Cast<ListMatcher>().matchers[0].get();
			}
			auto index = sequence.get().first_set.sequence_check_index;
			alternative_checks.push_back(index ? sequence_checks[index - 1].get() : nullptr);
		}
		check->SetAlternativeChecks(std::move(alternative_checks));
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
