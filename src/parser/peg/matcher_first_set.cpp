#include "duckdb/parser/peg/matcher.hpp"
#include "duckdb/parser/peg/matcher/list.hpp"
#include "duckdb/common/reference_map.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// FIRST sets
//===--------------------------------------------------------------------===//
bool MatcherTokenSet::MergeTokens(const MatcherTokenSet &other) {
	bool changed = false;
	if (other.any && !any) {
		any = changed = true;
	}
	if ((class_mask | other.class_mask) != class_mask) {
		class_mask |= other.class_mask;
		changed = true;
	}
	if (other.literals.size() > literals.size()) {
		literals.resize(other.literals.size(), 0);
	}
	for (idx_t i = 0; i < other.literals.size(); i++) {
		auto merged = literals[i] | other.literals[i];
		if (merged != literals[i]) {
			literals[i] = merged;
			changed = true;
		}
	}
	return changed;
}

void MatcherTokenSet::AddLiteral(idx_t literal_id) {
	auto word = literal_id / 64;
	if (word >= literals.size()) {
		literals.resize(word + 1, 0);
	}
	literals[word] |= uint64_t(1) << (literal_id % 64);
}

bool MatcherTokenSet::IsEmpty() const {
	if (any || class_mask) {
		return false;
	}
	for (auto word : literals) {
		if (word) {
			return false;
		}
	}
	return true;
}

bool MatcherTokenSet::Equals(const MatcherTokenSet &other) const {
	if (any != other.any || class_mask != other.class_mask) {
		return false;
	}
	auto words = MaxValue(literals.size(), other.literals.size());
	for (idx_t i = 0; i < words; i++) {
		auto left = i < literals.size() ? literals[i] : 0;
		auto right = i < other.literals.size() ? other.literals[i] : 0;
		if (left != right) {
			return false;
		}
	}
	return true;
}

void MatcherFirstSet::Finalize() {
	computed = true;
	if (one.any || second.any) {
		first_suffices = true;
		return;
	}
	// ONE covers FIRST: a token that passes the FIRST check can always be a one-token match
	if ((one.class_mask & class_mask) != class_mask) {
		first_suffices = false;
		return;
	}
	for (idx_t i = 0; i < literals.size(); i++) {
		auto one_word = i < one.literals.size() ? one.literals[i] : 0;
		if ((one_word & literals[i]) != literals[i]) {
			first_suffices = false;
			return;
		}
	}
	first_suffices = true;
}

static bool RejectTail(MatchState &state, idx_t stop_position);

bool MatcherFirstSet::MightMatchSecond(MatchState &state) const {
	auto &iterator = state.token_iterator;
	auto token = iterator.Current();
	D_ASSERT(token);
	bool continues = one.Contains(*token, [&]() { return iterator.CurrentLiteralInfo(*table); });
	if (!continues) {
		auto next = iterator.Peek(1);
		// the autocomplete cursor can stand for any token
		continues = next && (next->type == TokenType::END_OF_INPUT_AUTOCOMPLETE ||
		                     second.Contains(*next, [&]() { return iterator.PeekLiteralInfo(*table, 1); }));
	}
	if (!continues) {
		// the matcher would have consumed the current token before failing: keep the error position
		if (iterator.Position() + 1 > state.context.max_token_index) {
			state.context.max_token_index = iterator.Position() + 1;
		}
		return false;
	}
	if (!sequence_check_index || !state.context.sequence_checks) {
		return true;
	}
	auto &check = *(*state.context.sequence_checks)[sequence_check_index - 1];
	for (idx_t i = 0; i < check.later_tokens.size(); i++) {
		auto ahead = i + 2;
		auto later = iterator.Peek(ahead);
		// the autocomplete cursor can stand for any token
		if (!later || later->type == TokenType::END_OF_INPUT_AUTOCOMPLETE) {
			return true;
		}
		if (!check.later_tokens[i].Contains(*later, [&]() { return iterator.PeekLiteralInfo(*table, ahead); })) {
			return RejectTail(state, iterator.Position() + ahead);
		}
	}
	if (check.leading_choice && !MightMatchLeadingChoice(check, state)) {
		return false;
	}
	return !check.has_tail || MightMatchTail(check, state);
}

bool MatcherFirstSet::MightMatchLeadingChoice(const MatcherSequenceCheck &check, MatchState &state) const {
	auto &iterator = state.token_iterator;
	auto token = iterator.Current();
	auto next = iterator.Peek(1);
	// the autocomplete cursor can stand for any token
	if (!next || next->type == TokenType::END_OF_INPUT_AUTOCOMPLETE) {
		return true;
	}
	auto current_literal = [&]() {
		return iterator.CurrentLiteralInfo(*table);
	};
	auto next_literal = [&]() {
		return iterator.PeekLiteralInfo(*table, 1);
	};
	for (auto &alternative : check.leading_choice->matchers) {
		auto &first = alternative.get().first_set;
		if (!first.computed || first.any || first.nullable) {
			return true;
		}
		if (!first.Contains(*token, current_literal)) {
			continue;
		}
		if (first.second.Contains(*next, next_literal)) {
			return true;
		}
		// a one-token alternative is followed by the rest of the sequence
		if (first.one.Contains(*token, current_literal) &&
		    (check.rest_nullable || check.rest_first.Contains(*next, next_literal))) {
			return true;
		}
	}
	// the sequence would have consumed the current token before failing: keep the error position
	if (iterator.Position() + 1 > state.context.max_token_index) {
		state.context.max_token_index = iterator.Position() + 1;
	}
	return false;
}

bool MatcherFirstSet::Merge(const MatcherFirstSet &other) {
	bool changed = MergeTokens(other);
	changed |= one.MergeTokens(other.one);
	changed |= second.MergeTokens(other.second);
	return changed;
}

//! Calls fun(child) for every child of a built-in composite matcher
template <class FUN>
static void ForEachChild(Matcher &matcher, FUN &&fun) {
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
static bool IsComposite(const Matcher &matcher) {
	if (!matcher.IsStructural()) {
		return false;
	}
	auto type = matcher.Type();
	return type == MatcherType::LIST || type == MatcherType::CHOICE || type == MatcherType::OPTIONAL ||
	       type == MatcherType::REPEAT;
}

//! Recomputes the lookahead sets of a composite matcher from its children, returns whether anything changed.
//! ONE holds the first tokens of one-token matches and SECOND the second tokens of longer matches.
static bool UpdateFirstSet(Matcher &matcher) {
	auto &first = matcher.first_set;
	bool changed = false;
	bool nullable;
	switch (matcher.Type()) {
	case MatcherType::LIST: {
		auto &children = matcher.Cast<ListMatcher>().matchers;
		// the first non-nullable element ends the FIRST set of a sequence
		nullable = true;
		for (idx_t i = 0; i < children.size() && nullable; i++) {
			auto &child_first = children[i].get().first_set;
			changed |= first.MergeTokens(child_first);
			// the element can be followed by the FIRST set of the rest of the sequence
			MatcherTokenSet suffix_first;
			bool suffix_nullable = true;
			for (idx_t j = i + 1; j < children.size(); j++) {
				auto &suffix_child = children[j].get().first_set;
				suffix_first.MergeTokens(suffix_child);
				if (!suffix_child.nullable) {
					suffix_nullable = false;
					break;
				}
			}
			if (suffix_nullable) {
				changed |= first.one.MergeTokens(child_first.one);
			}
			changed |= first.second.MergeTokens(child_first.second);
			if (!child_first.one.IsEmpty()) {
				changed |= first.second.MergeTokens(suffix_first);
			}
			nullable = child_first.nullable;
		}
		break;
	}
	case MatcherType::CHOICE:
		nullable = false;
		for (auto &child : matcher.Cast<ChoiceMatcher>().matchers) {
			auto &child_first = child.get().first_set;
			changed |= first.Merge(child_first);
			nullable = nullable || child_first.nullable;
		}
		break;
	case MatcherType::OPTIONAL:
		changed |= first.Merge(matcher.Cast<OptionalMatcher>().GetChildMatcher().first_set);
		nullable = true;
		break;
	case MatcherType::REPEAT: {
		auto &child_first = matcher.Cast<RepeatMatcher>().GetChildMatcher().first_set;
		changed |= first.Merge(child_first);
		// a one-token element can be followed by the next repetition
		if (!child_first.one.IsEmpty()) {
			changed |= first.second.MergeTokens(child_first);
		}
		nullable = child_first.nullable;
		break;
	}
	default:
		throw InternalException("UpdateFirstSet called on a matcher that is not composite");
	}
	if (nullable && !first.nullable) {
		first.nullable = true;
		changed = true;
	}
	return changed;
}

struct FirstSetTraversalEntry {
	explicit FirstSetTraversalEntry(Matcher &matcher) : matcher(matcher) {
	}

	reference<Matcher> matcher;
	bool expanded = false;
};

void ComputeFirstSets(Matcher &root, const GrammarLiteralTable &table) {
	// collect the reachable matchers, children before their parents (except for recursive references)
	vector<reference<Matcher>> matchers;
	vector<reference<Matcher>> composites;
	reference_set_t<Matcher> seen;
	vector<FirstSetTraversalEntry> pending;
	pending.emplace_back(root);
	seen.insert(root);
	while (!pending.empty()) {
		auto &entry = pending.back();
		auto &matcher = entry.matcher.get();
		if (entry.expanded) {
			pending.pop_back();
			matchers.push_back(matcher);
			continue;
		}
		entry.expanded = true;
		// entry is invalidated by the pushes below
		ForEachChild(matcher, [&](Matcher &child) {
			if (seen.insert(child).second) {
				pending.emplace_back(child);
			}
		});
	}
	for (auto &entry : matchers) {
		auto &matcher = entry.get();
		auto &first = matcher.first_set;
		first = MatcherFirstSet();
		first.table = &table;
		if (IsComposite(matcher)) {
			composites.push_back(matcher);
		} else if (matcher.IsAtomic()) {
			// matchers that only describe FIRST may consume any number of tokens
			first.one.any = true;
			first.second.any = true;
			static_cast<const AtomicMatcher &>(matcher).InitializeFirstSet(first, table);
		} else {
			// custom and derived matchers: no information
			first.SetAny();
		}
	}
	// composite matchers: least fixed point, the grammar is recursive
	// children are visited before their parents, so only recursive references need another pass
	bool changed = true;
	while (changed) {
		changed = false;
		for (auto &matcher : composites) {
			changed |= UpdateFirstSet(matcher.get());
		}
	}
	for (auto &entry : matchers) {
		entry.get().first_set.Finalize();
	}
}

//===--------------------------------------------------------------------===//
// Tail checks
//===--------------------------------------------------------------------===//
static constexpr idx_t MAX_TAIL_SCAN = 128;

namespace {
enum class BracketKind : uint8_t { NONE, OPEN, CLOSE };
} // namespace

static BracketKind TokenBracket(const MatcherToken &token) {
	if (token.length != 1) {
		return BracketKind::NONE;
	}
	switch (token.text[0]) {
	case '(':
	case '[':
	case '{':
		return BracketKind::OPEN;
	case ')':
	case ']':
	case '}':
		return BracketKind::CLOSE;
	default:
		return BracketKind::NONE;
	}
}

//! String literals also carry the WORD class for name positions; alphabets mark string consumption with STRING
static uint8_t TailTokenClass(const MatcherToken &token) {
	return token.type == TokenType::STRING_LITERAL ? MatcherTokenClass::STRING : token.token_class;
}

//! A skipped sequence could have come as far as the token that stopped the scan
static bool RejectTail(MatchState &state, idx_t stop_position) {
	if (stop_position > state.context.tail_rejected_until) {
		state.context.tail_rejected_until = stop_position;
	}
	return false;
}

bool MatcherFirstSet::MightMatchTail(const MatcherSequenceCheck &check, MatchState &state) const {
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
			if (depth == 0 && check.bracket_ends_prefix) {
				// the prefix consumes no brackets: this one has to start the element
				auto literal_id = iterator.PeekLiteralInfo(*table, ahead).LiteralId();
				if (literal_id && check.required.HasLiteral(literal_id)) {
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
		if (token_class & check.alphabet.class_mask) {
			continue;
		}
		auto literal_id = iterator.PeekLiteralInfo(*table, ahead).LiteralId();
		if (literal_id && check.alphabet.HasLiteral(literal_id)) {
			continue;
		}
		// the first token the prefix cannot consume has to start the element
		if (token_class & check.required.class_mask) {
			return true;
		}
		if (literal_id && check.required.HasLiteral(literal_id)) {
			return true;
		}
		return RejectTail(state, iterator.Position() + ahead);
	}
	return true;
}

} // namespace duckdb
