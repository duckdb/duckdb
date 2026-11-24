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
	return MightMatchSequence(state);
}

bool MatcherFirstSet::MightMatchSequence(MatchState &state) const {
	if (!sequence_check_index || !state.context.sequence_checks) {
		return true;
	}
	return (*state.context.sequence_checks)[sequence_check_index - 1]->MightMatch(state);
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
			// the element can be followed by the FIRST set of the rest of the sequence, up to and including its
			// first element that is not nullable
			idx_t suffix_end = i + 1;
			bool suffix_nullable = true;
			for (; suffix_end < children.size(); suffix_end++) {
				if (!children[suffix_end].get().first_set.nullable) {
					suffix_nullable = false;
					suffix_end++;
					break;
				}
			}
			if (suffix_nullable) {
				changed |= first.one.MergeTokens(child_first.one);
			}
			changed |= first.second.MergeTokens(child_first.second);
			if (!child_first.one.IsEmpty()) {
				for (idx_t j = i + 1; j < suffix_end; j++) {
					changed |= first.second.MergeTokens(children[j].get().first_set);
				}
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

//! A composite matcher in the fixed point of the FIRST sets
struct FirstSetComposite {
	explicit FirstSetComposite(Matcher &matcher) : matcher(matcher) {
	}

	reference<Matcher> matcher;
	//! position of the matcher among the reachable matchers
	idx_t position = 0;
	//! the positions of its children are child_positions[children_begin, children_end)
	idx_t children_begin = 0;
	idx_t children_end = 0;
	//! sum of the versions of the children at the last update
	uint64_t children_version = 0;
	bool updated = false;
};

void ComputeFirstSets(Matcher &root, const GrammarLiteralTable &table) {
	// collect the reachable matchers, children before their parents (except for recursive references)
	vector<reference<Matcher>> matchers;
	// position of a matcher in matchers, set when the matcher is added
	reference_map_t<Matcher, idx_t> positions;
	vector<FirstSetTraversalEntry> pending;
	pending.emplace_back(root);
	positions.emplace(root, 0);
	while (!pending.empty()) {
		auto &entry = pending.back();
		auto &matcher = entry.matcher.get();
		if (entry.expanded) {
			pending.pop_back();
			positions[matcher] = matchers.size();
			matchers.push_back(matcher);
			continue;
		}
		entry.expanded = true;
		// entry is invalidated by the pushes below
		ForEachChild(matcher, [&](Matcher &child) {
			if (positions.emplace(child, 0).second) {
				pending.emplace_back(child);
			}
		});
	}
	// the composite matchers with the positions of their children, as ranges of one array
	vector<FirstSetComposite> composites;
	vector<idx_t> child_positions;
	child_positions.reserve(2 * matchers.size());
	for (auto &entry : matchers) {
		auto &matcher = entry.get();
		auto &first = matcher.first_set;
		first = MatcherFirstSet();
		first.table = &table;
		if (IsComposite(matcher)) {
			FirstSetComposite composite(matcher);
			composite.position = positions[matcher];
			composite.children_begin = child_positions.size();
			ForEachChild(matcher, [&](Matcher &child) { child_positions.push_back(positions[child]); });
			composite.children_end = child_positions.size();
			composites.push_back(composite);
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
	// a matcher's version counts the changes of its sets; versions only grow, so a matcher whose children have the
	// same sum of versions as at its last update has nothing to merge
	vector<uint64_t> versions(matchers.size(), 0);
	bool changed = true;
	while (changed) {
		changed = false;
		for (auto &composite : composites) {
			uint64_t children_version = 0;
			for (idx_t child_idx = composite.children_begin; child_idx < composite.children_end; child_idx++) {
				children_version += versions[child_positions[child_idx]];
			}
			if (composite.updated && children_version == composite.children_version) {
				continue;
			}
			composite.updated = true;
			composite.children_version = children_version;
			if (UpdateFirstSet(composite.matcher.get())) {
				versions[composite.position]++;
				changed = true;
			}
		}
	}
	for (auto &entry : matchers) {
		entry.get().first_set.Finalize();
	}
}

} // namespace duckdb
