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

//! Tokens a matcher consumes outside of balanced bracket pairs; unsafe when they cannot be determined. The literals
//! are a bitset in the analysis' pool, at the slot of the set
struct ConsumedTokens {
	explicit ConsumedTokens(idx_t slot_p) : slot(slot_p) {
	}

	idx_t slot;
	uint8_t class_mask = 0;
	bool safe = true;
	//! every bracket the matcher consumes belongs to a balanced pair it consumes as well
	bool balanced = true;
	//! the tokens include balanced bracket pairs, which the tail scan skips
	bool brackets = false;

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
	SequenceCheckNode(Matcher &matcher_p, idx_t index) : matcher(matcher_p), consumed(index) {
	}

	Matcher &matcher;
	//! child nodes of a composite matcher, in order
	vector<idx_t> children;
	ConsumedTokens consumed;
	//! counts the changes of consumed
	uint64_t version = 0;
	//! sum of the versions of the children when consumed was last derived from them
	uint64_t children_version = NumericLimits<uint64_t>::Maximum();
	//! set when the matcher consumes exactly one bracket literal
	BracketKind bracket = BracketKind::NONE;
	idx_t bracket_pair = 0;
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
	//! length counts the tokens so far; only the tokens after the first two are recorded, in later_tokens
	bool AppendFixedPrefix(idx_t node_index, idx_t &length, vector<MatcherTokenSet> &later_tokens) const;
	//! The check of a sequence, created on first use
	MatcherSequenceCheck &CheckFor(SequenceCheckNode &node) const;

	//! What a matcher that is not a built-in composite consumes
	void InitializeLeaf(SequenceCheckNode &node);
	//! Whether a literal is a bracket, and of which pair of BRACKET_PAIRS
	BracketKind BracketLiteral(idx_t literal_id, idx_t &pair_idx) const;
	//! The bracket a matcher consumes when it matches exactly one literal, a bracket
	BracketKind SingleBracket(const MatcherFirstSet &first, idx_t &pair_idx) const;
	//! Merges the tokens children[covered, end) consume outside of the bracket pairs among them into prefix, which
	//! holds those of children[0, covered). False while a bracket pair is still open at end
	bool ExtendPrefix(const vector<idx_t> &children, idx_t end, idx_t &covered, ConsumedTokens &prefix, bool &changed);
	bool Update(idx_t node_index);
	//! The composite nodes, each after the children it reaches without a recursive reference
	vector<idx_t> CompositesChildrenFirst() const;
	//! Merges the tokens of source into target, returns whether target changed
	bool Merge(ConsumedTokens &target, const ConsumedTokens &source);
	//! Adds the tokens of a token set to target
	void AddTokens(ConsumedTokens &target, const MatcherTokenSet &tokens);
	static void SetBit(vector<uint64_t> &bits, idx_t index) {
		auto word = index / 64;
		if (word >= bits.size()) {
			bits.resize(word + 1, 0);
		}
		bits[word] |= uint64_t(1) << (index % 64);
	}
	static uint64_t Word(const vector<uint64_t> &bits, idx_t word) {
		return word < bits.size() ? bits[word] : 0;
	}
	//! The literals of one word of the bitset whose token has a class in class_mask
	uint64_t ClassLiterals(uint8_t class_mask, idx_t word) const {
		uint64_t result = 0;
		for (idx_t bit = 0; bit < TOKEN_CLASS_BITS; bit++) {
			if (class_mask & (1U << bit)) {
				result |= Word(class_literals[bit], word);
			}
		}
		return result;
	}
	//! No token can be consumed by the prefix and start the element
	bool Disjoint(const ConsumedTokens &prefix, const MatcherTokenSet &required) const;

private:
	//! The number of bits of a token class
	static constexpr idx_t TOKEN_CLASS_BITS = 8;
	//! A leading choice with more alternatives is not checked per alternative
	static constexpr idx_t MAX_LEADING_ALTERNATIVES = 6;
	//! The number of leading tokens of a sequence that are recorded
	static constexpr idx_t MAX_FIXED_PREFIX = 6;

	const GrammarLiteralTable &table;
	vector<SequenceCheckNode> nodes;
	//! literal ids of the brackets of each pair of BRACKET_PAIRS
	vector<uint16_t> open_literals;
	vector<uint16_t> close_literals;
	//! for every bit of a token class, the literals whose token has that bit, as a bitset over literal ids
	vector<vector<uint64_t>> class_literals;
	//! the literals of every ConsumedTokens, literal_words words per slot
	vector<uint64_t> literal_pool;
	idx_t literal_words = 0;
	//! the opening and the closing bracket literals
	vector<uint64_t> open_brackets;
	vector<uint64_t> close_brackets;
};

SequenceCheckAnalysis::SequenceCheckAnalysis(const vector<unique_ptr<Matcher>> &all_matchers,
                                             const GrammarLiteralTable &table_p)
    : table(table_p) {
	for (auto &bracket_pair : BRACKET_PAIRS) {
		open_literals.push_back(table.Lookup(&bracket_pair.open, 1).LiteralId());
		close_literals.push_back(table.Lookup(&bracket_pair.close, 1).LiteralId());
	}
	class_literals.resize(TOKEN_CLASS_BITS);
	table.ForEachLiteralText([&](const string &text, const LiteralInfo &info) {
		uint8_t token_class;
		if (info.IsKeyword()) {
			token_class = MatcherTokenClass::WORD;
		} else if (!text.empty() && Tokenizer::CharacterIsKeyword(text[0])) {
			token_class = MatcherTokenClass::WORD | MatcherTokenClass::IDENTIFIER;
		} else {
			token_class = ComputeMatcherTokenClass(TokenType::OPERATOR, text);
		}
		for (idx_t bit = 0; bit < TOKEN_CLASS_BITS; bit++) {
			if (token_class & (1U << bit)) {
				SetBit(class_literals[bit], info.LiteralId());
			}
		}
	});
	for (idx_t pair_idx = 0; pair_idx < open_literals.size(); pair_idx++) {
		// a bracket the grammar does not use has no literal id
		if (open_literals[pair_idx]) {
			SetBit(open_brackets, open_literals[pair_idx]);
		}
		if (close_literals[pair_idx]) {
			SetBit(close_brackets, close_literals[pair_idx]);
		}
	}
	// only reachable matchers have FIRST sets. A matcher's packrat id is its position in the allocator
	const auto no_node = NumericLimits<idx_t>::Maximum();
	vector<idx_t> node_of_matcher(all_matchers.size(), no_node);
	nodes.reserve(all_matchers.size());
	for (idx_t matcher_idx = 0; matcher_idx < all_matchers.size(); matcher_idx++) {
		auto &matcher = *all_matchers[matcher_idx];
		auto &first = matcher.first_set;
		if (!first.computed) {
			continue;
		}
		D_ASSERT(matcher.GetPackratId().GetIndex() == matcher_idx);
		node_of_matcher[matcher_idx] = nodes.size();
		nodes.emplace_back(matcher, nodes.size());
		literal_words =
		    MaxValue<idx_t>(literal_words, MaxValue<idx_t>(first.literals.size(), first.second.literals.size()));
	}
	// one bitset per node and one for the prefix of the sequence being checked
	literal_pool.resize((nodes.size() + 1) * literal_words, 0);
	for (idx_t node_index = 0; node_index < nodes.size(); node_index++) {
		auto &node = nodes[node_index];
		auto &matcher = node.matcher;
		if (IsCompositeMatcher(matcher)) {
			bool complete = true;
			idx_t child_count = 0;
			ForEachChildMatcher(matcher, [&](Matcher &) { child_count++; });
			node.children.reserve(child_count);
			ForEachChildMatcher(matcher, [&](Matcher &child) {
				auto packrat_id = child.GetPackratId();
				auto child_node = no_node;
				// a matcher of another allocator is not part of this analysis
				if (packrat_id.IsValid() && packrat_id.GetIndex() < all_matchers.size() &&
				    all_matchers[packrat_id.GetIndex()].get() == &child) {
					child_node = node_of_matcher[packrat_id.GetIndex()];
				}
				if (child_node == no_node) {
					complete = false;
				} else {
					node.children.push_back(child_node);
				}
			});
			if (!complete) {
				node.children.clear();
				node.consumed.MarkUnbalanced();
			}
		} else {
			InitializeLeaf(node);
		}
	}
}

void SequenceCheckAnalysis::InitializeLeaf(SequenceCheckNode &node) {
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
	AddTokens(node.consumed, first);
	AddTokens(node.consumed, first.second);
	node.bracket = SingleBracket(first, node.bracket_pair);
	if (node.bracket != BracketKind::NONE) {
		// a bracket is only consumed as part of a balanced pair in one sequence
		node.consumed.MarkUnbalanced();
	}
}

bool SequenceCheckAnalysis::Merge(ConsumedTokens &target, const ConsumedTokens &source) {
	bool changed = false;
	if (target.balanced && !source.balanced) {
		target.balanced = false;
		changed = true;
	}
	if (!target.safe) {
		// the tokens of an unsafe matcher are never used
		return changed;
	}
	if (!source.safe) {
		target.safe = false;
		return true;
	}
	if ((target.class_mask | source.class_mask) != target.class_mask) {
		target.class_mask |= source.class_mask;
		changed = true;
	}
	// both slots are literal_words words of the pool
	D_ASSERT((MaxValue(target.slot, source.slot) + 1) * literal_words <= literal_pool.size());
	auto target_words = literal_pool.data() + target.slot * literal_words;
	auto source_words = literal_pool.data() + source.slot * literal_words;
	for (idx_t word = 0; word < literal_words; word++) {
		auto merged = target_words[word] | source_words[word];
		if (merged != target_words[word]) {
			target_words[word] = merged;
			changed = true;
		}
	}
	if (!target.brackets && source.brackets) {
		target.brackets = true;
		changed = true;
	}
	return changed;
}

void SequenceCheckAnalysis::AddTokens(ConsumedTokens &target, const MatcherTokenSet &tokens) {
	target.class_mask |= tokens.class_mask;
	auto target_base = target.slot * literal_words;
	for (idx_t word = 0; word < tokens.literals.size(); word++) {
		literal_pool[target_base + word] |= tokens.literals[word];
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

bool SequenceCheckAnalysis::ExtendPrefix(const vector<idx_t> &children, idx_t end, idx_t &covered,
                                         ConsumedTokens &prefix, bool &changed) {
	// nothing changes any more once the prefix is neither safe nor balanced
	while (covered < end && (prefix.safe || prefix.balanced)) {
		auto &child = nodes[children[covered]];
		if (child.bracket == BracketKind::NONE) {
			changed |= Merge(prefix, child.consumed);
			covered++;
			continue;
		}
		if (child.bracket == BracketKind::CLOSE) {
			changed |= prefix.MarkUnbalanced();
			break;
		}
		// find the closing bracket of the same pair in the whole sequence, nested pairs included
		idx_t depth = 0;
		idx_t close = covered + 1;
		for (; close < children.size(); close++) {
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
		if (close >= children.size()) {
			changed |= prefix.MarkUnbalanced();
			break;
		}
		if (close >= end) {
			return false;
		}
		// the interior is skipped at runtime, so only its brackets matter: they have to be balanced
		for (idx_t interior = covered + 1; interior < close; interior++) {
			auto &interior_node = nodes[children[interior]];
			if (interior_node.bracket == BracketKind::NONE && !interior_node.consumed.balanced) {
				changed |= prefix.MarkUnbalanced();
			}
		}
		if (!prefix.brackets) {
			prefix.brackets = true;
			changed = true;
		}
		covered = close + 1;
	}
	return true;
}

bool SequenceCheckAnalysis::AppendFixedPrefix(idx_t node_index, idx_t &length,
                                              vector<MatcherTokenSet> &later_tokens) const {
	auto &node = nodes[node_index];
	auto &matcher = node.matcher;
	auto &first = matcher.first_set;
	if (length >= MAX_FIXED_PREFIX || !first.computed || first.any || first.nullable) {
		return false;
	}
	// the FIRST and SECOND sets cover the first two tokens
	const bool record = length >= 2;
	if (matcher.IsAtomic()) {
		// single-token atomics: FIRST is exactly the one token they consume
		if (first.one.any || first.second.any || !first.second.IsEmpty()) {
			return false;
		}
		if (record) {
			later_tokens.emplace_back(static_cast<const MatcherTokenSet &>(first));
		}
		length++;
		return true;
	}
	if (!IsCompositeMatcher(matcher)) {
		return false;
	}
	switch (matcher.Type()) {
	case MatcherType::LIST:
		for (auto child : node.children) {
			if (!AppendFixedPrefix(child, length, later_tokens)) {
				return false;
			}
		}
		return !node.children.empty();
	case MatcherType::CHOICE: {
		// alternatives that are all single tokens: any of them
		if (node.children.empty()) {
			return false;
		}
		for (auto child : node.children) {
			auto &alternative = nodes[child].matcher;
			auto &alternative_first = alternative.first_set;
			if (!alternative.IsAtomic() || alternative_first.any || alternative_first.one.any ||
			    alternative_first.second.any || !alternative_first.second.IsEmpty()) {
				return false;
			}
		}
		if (record) {
			MatcherTokenSet tokens;
			for (auto child : node.children) {
				tokens.MergeTokens(nodes[child].matcher.first_set);
			}
			later_tokens.push_back(std::move(tokens));
		}
		length++;
		return true;
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

vector<idx_t> SequenceCheckAnalysis::CompositesChildrenFirst() const {
	vector<idx_t> order;
	order.reserve(nodes.size());
	// 0: not visited, otherwise one more than the number of children already visited
	vector<uint32_t> progress(nodes.size(), 0);
	vector<idx_t> path;
	for (idx_t root = 0; root < nodes.size(); root++) {
		if (progress[root]) {
			continue;
		}
		progress[root] = 1;
		path.push_back(root);
		while (!path.empty()) {
			auto node_index = path.back();
			auto &children = nodes[node_index].children;
			auto next_child = progress[node_index] - 1;
			if (next_child < children.size()) {
				progress[node_index]++;
				auto child = children[next_child];
				if (!progress[child]) {
					progress[child] = 1;
					path.push_back(child);
				}
				continue;
			}
			path.pop_back();
			if (!children.empty()) {
				order.push_back(node_index);
			}
		}
	}
	return order;
}

bool SequenceCheckAnalysis::Update(idx_t node_index) {
	auto &node = nodes[node_index];
	if (node.matcher.Type() == MatcherType::LIST) {
		bool changed = false;
		idx_t covered = 0;
		ExtendPrefix(node.children, node.children.size(), covered, node.consumed, changed);
		return changed;
	}
	bool changed = false;
	for (auto child : node.children) {
		changed |= Merge(node.consumed, nodes[child].consumed);
	}
	return changed;
}

bool SequenceCheckAnalysis::Disjoint(const ConsumedTokens &prefix, const MatcherTokenSet &required) const {
	auto alphabet_base = prefix.slot * literal_words;
	// A token's class can have two bits (an identifier is WORD | IDENTIFIER), so two masks without a common bit can
	// still accept the same token. Test every class the tokenizer assigns (ComputeMatcherTokenClass) instead.
	static constexpr uint8_t TOKEN_CLASSES[] = {
	    MatcherTokenClass::WORD,     MatcherTokenClass::WORD | MatcherTokenClass::IDENTIFIER,
	    MatcherTokenClass::STRING,   MatcherTokenClass::NUMBER,
	    MatcherTokenClass::OPERATOR, MatcherTokenClass::OPERATOR | MatcherTokenClass::GENERIC_OPERATOR};
	for (auto token_class : TOKEN_CLASSES) {
		if ((token_class & prefix.class_mask) && (token_class & required.class_mask)) {
			return false;
		}
	}
	auto &required_literals = required.literals;
	D_ASSERT(required_literals.size() <= literal_words);
	for (idx_t word = 0; word < required_literals.size(); word++) {
		// literals the required token must not be: the alphabet's, those of a class of the alphabet, a closing
		// bracket, and an opening one when the scan skips the bracket pairs the prefix consumes
		uint64_t excluded =
		    literal_pool[alphabet_base + word] | ClassLiterals(prefix.class_mask, word) | Word(close_brackets, word);
		if (prefix.brackets) {
			excluded |= Word(open_brackets, word);
		}
		if (required_literals[word] & excluded) {
			return false;
		}
	}
	for (idx_t word = 0; word < literal_words; word++) {
		if (literal_pool[alphabet_base + word] & ClassLiterals(required.class_mask, word)) {
			return false;
		}
	}
	return true;
}

vector<CheckedSequence> SequenceCheckAnalysis::CreateSequenceChecks() {
	// least fixed point over the recursive grammar: sweep until no node changes. Children come before their
	// parents in the sweep, so only recursive references need another one
	auto order = CompositesChildrenFirst();
	bool changed = true;
	while (changed) {
		changed = false;
		for (auto node_index : order) {
			auto &node = nodes[node_index];
			// versions only grow, so an unchanged sum means that no child changed
			uint64_t children_version = 0;
			for (auto child : node.children) {
				children_version += nodes[child].version;
			}
			if (children_version == node.children_version) {
				continue;
			}
			node.children_version = children_version;
			if (Update(node_index)) {
				node.version++;
				changed = true;
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
		// the tokens of children[0, covered), extended as the element moves on
		ConsumedTokens prefix(nodes.size());
		bool prefix_cleared = false;
		idx_t covered = 0;
		bool changed = false;
		for (idx_t element_idx = 1; element_idx < children.size(); element_idx++) {
			auto &element_first = nodes[children[element_idx]].matcher.first_set;
			if (!element_first.computed || element_first.any || element_first.nullable) {
				continue;
			}
			// a one-token prefix is already covered by the SECOND set
			if (element_idx == 1 && nodes[children[0]].matcher.IsAtomic()) {
				continue;
			}
			if (!prefix_cleared) {
				for (idx_t word = prefix.slot * literal_words; word < literal_pool.size(); word++) {
					literal_pool[word] = 0;
				}
				prefix_cleared = true;
			}
			const bool complete = ExtendPrefix(children, element_idx, covered, prefix, changed);
			if (!prefix.safe) {
				// every longer prefix is unsafe as well
				break;
			}
			if (!complete) {
				// the element is inside a bracket pair of the prefix
				continue;
			}
			if (!Disjoint(prefix, element_first)) {
				continue;
			}
			// the element has to start with a token of its FIRST set
			const MatcherTokenSet &required = element_first;
			MatcherTokenSet alphabet;
			alphabet.class_mask = prefix.class_mask;
			alphabet.literals.reserve(literal_words);
			for (idx_t word = prefix.slot * literal_words; word < literal_pool.size(); word++) {
				alphabet.literals.push_back(literal_pool[word]);
			}
			CheckFor(node).SetTail(std::move(alphabet), required, !prefix.brackets);
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
		idx_t length = 0;
		vector<MatcherTokenSet> later_tokens;
		AppendFixedPrefix(node_index, length, later_tokens);
		if (later_tokens.empty()) {
			continue;
		}
		CheckFor(node).SetLaterTokens(std::move(later_tokens));
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
