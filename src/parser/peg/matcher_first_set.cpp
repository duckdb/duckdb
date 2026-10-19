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

bool MatcherFirstSet::MightMatchSecond(MatchState &state) const {
	auto &iterator = state.token_iterator;
	auto token = iterator.Current();
	D_ASSERT(token);
	if (one.Contains(*token, [&]() { return iterator.CurrentLiteralInfo(*table); })) {
		return true;
	}
	auto next = iterator.Peek(1);
	// the autocomplete cursor can stand for any token
	if (next && next->type == TokenType::END_OF_INPUT_AUTOCOMPLETE) {
		return true;
	}
	if (next && second.Contains(*next, [&]() { return iterator.PeekLiteralInfo(*table, 1); })) {
		return true;
	}
	// the matcher would have consumed the current token before failing: keep the error position
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

} // namespace duckdb
