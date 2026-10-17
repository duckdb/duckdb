#include "duckdb/parser/peg/matcher.hpp"
#include "duckdb/parser/peg/matcher/identifier_matcher.hpp"
#include "duckdb/parser/peg/matcher/list.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// FIRST sets
//===--------------------------------------------------------------------===//
//! Merge the bitset other into target, returns whether anything changed
static bool MergeLiterals(uint64_t *target, const uint64_t *other, idx_t words) {
	uint64_t added = 0;
	for (idx_t i = 0; i < words; i++) {
		added |= other[i] & ~target[i];
		target[i] |= other[i];
	}
	return added != 0;
}

bool MatcherFirstSet::Merge(const MatcherFirstSet &other) {
	D_ASSERT(literal_words == other.literal_words);
	bool changed = false;
	if (other.any && !any) {
		any = changed = true;
	}
	if ((class_mask | other.class_mask) != class_mask) {
		class_mask |= other.class_mask;
		changed = true;
	}
	if ((exact_class_mask | other.exact_class_mask) != exact_class_mask) {
		exact_class_mask |= other.exact_class_mask;
		changed = true;
	}
	changed |= MergeLiterals(literals, other.literals, literal_words);
	return changed;
}

void MatcherFirstSet::AddLiteral(idx_t literal_id) {
	auto word = literal_id / 64;
	D_ASSERT(word < literal_words);
	literals[word] |= uint64_t(1) << (literal_id % 64);
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

//! Recomputes the FIRST set of a composite matcher from its children, returns whether anything changed
static bool UpdateFirstSet(Matcher &matcher) {
	auto &first = matcher.first_set;
	bool changed = false;
	bool nullable;
	switch (matcher.Type()) {
	case MatcherType::LIST:
		// the first non-nullable element ends the FIRST set of a sequence
		nullable = true;
		for (auto &child : matcher.Cast<ListMatcher>().matchers) {
			auto &child_first = child.get().first_set;
			changed |= first.Merge(child_first);
			if (!child_first.nullable) {
				nullable = false;
				break;
			}
		}
		break;
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

//===--------------------------------------------------------------------===//
// Second-token sets
//===--------------------------------------------------------------------===//
static bool MergeTokenSet(bool &any, uint8_t &mask, uint64_t *literals, bool other_any, uint8_t other_mask,
                          const uint64_t *other_literals, idx_t words) {
	bool changed = false;
	if (other_any && !any) {
		any = changed = true;
	}
	if ((mask | other_mask) != mask) {
		mask |= other_mask;
		changed = true;
	}
	changed |= MergeLiterals(literals, other_literals, words);
	return changed;
}

//! Merge a FIRST set into the identifier-start second-token set
static bool MergeIntoIdentSecond(MatcherFirstSet &target, const MatcherFirstSet &first) {
	return MergeTokenSet(target.ident_second_any, target.ident_second_class_mask, target.ident_second_literals,
	                     first.any, first.class_mask, first.literals, target.literal_words);
}

//! Merge another matcher's identifier-start second-token set into target
static bool MergeIdentSecond(MatcherFirstSet &target, const MatcherFirstSet &other) {
	return MergeTokenSet(target.ident_second_any, target.ident_second_class_mask, target.ident_second_literals,
	                     other.ident_second_any, other.ident_second_class_mask, other.ident_second_literals,
	                     target.literal_words);
}

//! Merge a FIRST set into a second-token set, returns whether anything changed
static bool MergeIntoSecond(MatcherFirstSet &target, const MatcherFirstSet &first) {
	return MergeTokenSet(target.second_any, target.second_class_mask, target.second_literals, first.any,
	                     first.class_mask, first.literals, target.literal_words);
}

//! Merge another matcher's second-token set into target, returns whether anything changed
static bool MergeSecond(MatcherFirstSet &target, const MatcherFirstSet &other) {
	return MergeTokenSet(target.second_any, target.second_class_mask, target.second_literals, other.second_any,
	                     other.second_class_mask, other.second_literals, target.literal_words);
}

static bool SetFlag(bool &flag, bool value) {
	if (value && !flag) {
		flag = true;
		return true;
	}
	return false;
}

//! Tracks, along a sequence, whether the elements so far can match nothing, exactly one token, or two or more tokens
//! (also restricted to matches whose first token is consumed by an identifier matcher)
struct SequencePrefix {
	bool nullable = true;
	bool one = false;
	bool multi = false;
	bool ident_one = false;
	bool ident_multi = false;

	void Append(const MatcherFirstSet &child) {
		bool new_one = (one && child.nullable) || (nullable && child.can_one);
		bool new_multi = multi || (one && (child.can_one || child.can_multi)) || (nullable && child.can_multi);
		bool new_ident_one = (ident_one && child.nullable) || (nullable && child.ident_can_one);
		bool new_ident_multi =
		    ident_multi || (ident_one && (child.can_one || child.can_multi)) || (nullable && child.ident_can_multi);
		nullable = nullable && child.nullable;
		one = new_one;
		multi = new_multi;
		ident_one = new_ident_one;
		ident_multi = new_ident_multi;
	}
	//! every match consumed two tokens before the next element: later elements cannot contribute anything
	bool Done() const {
		return !nullable && !one && !ident_one;
	}
};

//! Recomputes how many tokens a composite matcher can consume from its children, returns whether anything changed
static bool UpdateTokenCountFlags(Matcher &matcher) {
	bool changed = false;
	auto &target = matcher.first_set;
	switch (matcher.Type()) {
	case MatcherType::LIST: {
		SequencePrefix prefix;
		for (auto &child_ref : matcher.Cast<ListMatcher>().matchers) {
			prefix.Append(child_ref.get().first_set);
			if (prefix.Done()) {
				break;
			}
		}
		changed |= SetFlag(target.can_one, prefix.one);
		changed |= SetFlag(target.can_multi, prefix.multi);
		changed |= SetFlag(target.ident_can_one, prefix.ident_one);
		changed |= SetFlag(target.ident_can_multi, prefix.ident_multi);
		break;
	}
	case MatcherType::CHOICE:
		for (auto &child_ref : matcher.Cast<ChoiceMatcher>().matchers) {
			auto &child = child_ref.get().first_set;
			changed |= SetFlag(target.can_one, child.can_one);
			changed |= SetFlag(target.can_multi, child.can_multi);
			changed |= SetFlag(target.ident_can_one, child.ident_can_one);
			changed |= SetFlag(target.ident_can_multi, child.ident_can_multi);
		}
		break;
	case MatcherType::OPTIONAL: {
		auto &child = matcher.Cast<OptionalMatcher>().GetChildMatcher().first_set;
		changed |= SetFlag(target.can_one, child.can_one);
		changed |= SetFlag(target.can_multi, child.can_multi);
		changed |= SetFlag(target.ident_can_one, child.ident_can_one);
		changed |= SetFlag(target.ident_can_multi, child.ident_can_multi);
		break;
	}
	case MatcherType::REPEAT: {
		auto &child = matcher.Cast<RepeatMatcher>().GetChildMatcher().first_set;
		changed |= SetFlag(target.can_one, child.can_one);
		changed |= SetFlag(target.can_multi, child.can_multi || child.can_one);
		changed |= SetFlag(target.ident_can_one, child.ident_can_one);
		changed |= SetFlag(target.ident_can_multi, child.ident_can_multi || child.ident_can_one);
		break;
	}
	default:
		break;
	}
	return changed;
}

//! Calls fun(child) for every child whose second-token set the second-token set of a composite matcher reads: the
//! elements of a sequence up to the first one that cannot match nothing (later ones only contribute FIRST sets)
template <class FUN>
static void ForEachSecondSetChild(Matcher &matcher, FUN &&fun) {
	if (matcher.Type() != MatcherType::LIST) {
		ForEachChild(matcher, fun);
		return;
	}
	for (auto &child : matcher.Cast<ListMatcher>().matchers) {
		fun(child.get());
		if (!child.get().first_set.nullable) {
			break;
		}
	}
}

//! Recomputes the second-token sets of a composite matcher from its children, given the final token count flags;
//! returns whether anything changed
static bool UpdateSecondSet(Matcher &matcher) {
	bool changed = false;
	auto &target = matcher.first_set;
	switch (matcher.Type()) {
	case MatcherType::LIST: {
		SequencePrefix prefix;
		for (auto &child_ref : matcher.Cast<ListMatcher>().matchers) {
			auto &child = child_ref.get().first_set;
			// after a one-token prefix, the first token of the next element is the second token
			if (prefix.one) {
				changed |= MergeIntoSecond(target, child);
			}
			if (prefix.ident_one) {
				changed |= MergeIntoIdentSecond(target, child);
			}
			if (prefix.nullable) {
				changed |= MergeSecond(target, child);
				changed |= MergeIdentSecond(target, child);
			}
			prefix.Append(child);
			if (prefix.Done()) {
				break;
			}
		}
		break;
	}
	case MatcherType::CHOICE:
		for (auto &child_ref : matcher.Cast<ChoiceMatcher>().matchers) {
			auto &child = child_ref.get().first_set;
			changed |= MergeSecond(target, child);
			changed |= MergeIdentSecond(target, child);
		}
		break;
	case MatcherType::OPTIONAL: {
		auto &child = matcher.Cast<OptionalMatcher>().GetChildMatcher().first_set;
		changed |= MergeSecond(target, child);
		changed |= MergeIdentSecond(target, child);
		break;
	}
	case MatcherType::REPEAT: {
		auto &child = matcher.Cast<RepeatMatcher>().GetChildMatcher().first_set;
		changed |= MergeSecond(target, child);
		if (child.can_one) {
			// a one-token repetition followed by another repetition
			changed |= MergeIntoSecond(target, child);
		}
		changed |= MergeIdentSecond(target, child);
		if (child.ident_can_one) {
			changed |= MergeIntoIdentSecond(target, child);
		}
		break;
	}
	default:
		break;
	}
	return changed;
}

//! Least fixed point of update over the active composite matchers, given children before their parents: a matcher is
//! only revisited when one of its children changed, which happens through recursive references
template <class FUN>
static void ComputeFixedPoint(const vector<reference<Matcher>> &composites, const vector<idx_t> &parent_offsets,
                              const vector<idx_t> &parents, const vector<bool> &active, FUN &&update) {
	// inactive matchers are never queued
	vector<bool> queued(composites.size(), true);
	vector<idx_t> queue;
	queue.reserve(composites.size());
	for (idx_t i = 0; i < composites.size(); i++) {
		if (active[i]) {
			queue.push_back(i);
		}
	}
	for (idx_t head = 0; head < queue.size(); head++) {
		auto index = queue[head];
		queued[index] = false;
		if (!update(composites[index].get())) {
			continue;
		}
		for (auto p = parent_offsets[index]; p < parent_offsets[index + 1]; p++) {
			auto parent = parents[p];
			if (active[parent] && !queued[parent]) {
				queued[parent] = true;
				queue.push_back(parent);
			}
		}
	}
}

//! Least fixed point over the grammar; a superset of the tokens any atomic matcher can consume second on any path.
//! The token count flags are computed for every matcher, the second-token sets only for the matchers whose sets the
//! matchers with lookahead read.
static void ComputeSecondSets(vector<reference<Matcher>> &matchers, const vector<reference<Matcher>> &composites,
                              const vector<idx_t> &parent_offsets, const vector<idx_t> &parents) {
	for (auto &entry : matchers) {
		auto &matcher = entry.get();
		auto &first = matcher.first_set;
		if (IsComposite(matcher)) {
			continue;
		}
		if (first.any || !matcher.IsAtomic()) {
			first.can_one = first.can_multi = first.second_any = true;
			first.ident_can_one = first.ident_can_multi = first.ident_second_any = true;
			continue;
		}
		if (matcher.Type() == MatcherType::VARIABLE) {
			first.ident_can_one = true;
		}
		switch (matcher.Type()) {
		case MatcherType::KEYWORD:
		case MatcherType::VARIABLE:
		case MatcherType::OPERATOR:
		case MatcherType::NUMBER_LITERAL:
			first.can_one = true;
			break;
		case MatcherType::STRING_LITERAL:
			// adjacent string literals continue the literal
			first.can_one = first.can_multi = true;
			first.second_class_mask = MatcherTokenClass::STRING;
			break;
		default:
			first.can_one = first.can_multi = first.second_any = true;
			first.ident_can_one = first.ident_can_multi = first.ident_second_any = true;
			break;
		}
	}
	ComputeFixedPoint(composites, parent_offsets, parents, vector<bool>(composites.size(), true),
	                  UpdateTokenCountFlags);

	// the matchers whose second-token sets are read, starting from the matchers with lookahead
	vector<bool> needed(matchers.size(), false);
	vector<reference<Matcher>> pending;
	for (idx_t i = 0; i < matchers.size(); i++) {
		if (matchers[i].get().HasSecondTokenLookahead()) {
			needed[i] = true;
			pending.push_back(matchers[i]);
		}
	}
	while (!pending.empty()) {
		auto &matcher = pending.back().get();
		pending.pop_back();
		ForEachSecondSetChild(matcher, [&](Matcher &child) {
			auto child_index = child.first_set.compute_index;
			if (!needed[child_index]) {
				needed[child_index] = true;
				pending.push_back(child);
			}
		});
	}
	vector<bool> active(composites.size());
	for (idx_t i = 0; i < composites.size(); i++) {
		active[i] = needed[composites[i].get().first_set.compute_index];
	}
	ComputeFixedPoint(composites, parent_offsets, parents, active, UpdateSecondSet);
	for (auto &entry : matchers) {
		auto &first = entry.get().first_set;
		first.use_second = !first.nullable && !first.any && !first.can_one && !first.second_any;
		first.use_ident_second = !first.nullable && !first.any && !first.ident_can_one && !first.ident_second_any;
	}
}

unsafe_unique_array<uint64_t> ComputeFirstSets(Matcher &root, const GrammarLiteralTable &table) {
	// collect the reachable matchers, children before their parents (except for recursive references)
	// compute_index marks the matchers that were already seen
	static constexpr idx_t SEEN = DConstants::INVALID_INDEX - 1;
	vector<reference<Matcher>> matchers;
	vector<reference<Matcher>> composites;
	vector<FirstSetTraversalEntry> pending;
	pending.emplace_back(root);
	root.first_set.compute_index = SEEN;
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
			if (child.first_set.compute_index == DConstants::INVALID_INDEX) {
				child.first_set.compute_index = SEEN;
				pending.emplace_back(child);
			}
		});
	}
	// one literal bitset per matcher, and the second-token bitsets of the matchers that keep them
	auto words = table.LiteralCount() / 64 + 1;
	idx_t lookahead_count = 0;
	for (auto &matcher : matchers) {
		lookahead_count += matcher.get().HasSecondTokenLookahead();
	}
	auto storage = make_unsafe_uniq_array<uint64_t>((matchers.size() + 2 * lookahead_count) * words);
	// the second-token bitsets of every matcher, while the sets are computed
	auto second_storage = make_unsafe_uniq_array<uint64_t>(2 * matchers.size() * words);
	for (idx_t i = 0; i < matchers.size(); i++) {
		auto &matcher = matchers[i].get();
		auto &first = matcher.first_set;
		first = MatcherFirstSet();
		first.table = &table;
		first.compute_index = i;
		first.literal_words = words;
		first.literals = storage.get() + i * words;
		first.second_literals = second_storage.get() + 2 * i * words;
		first.ident_second_literals = first.second_literals + words;
		if (IsComposite(matcher)) {
			composites.push_back(matcher);
		} else if (matcher.IsAtomic()) {
			static_cast<const AtomicMatcher &>(matcher).InitializeFirstSet(first, table);
		} else {
			// custom and derived matchers: no information
			first.any = true;
		}
	}
	// the composite parents of every composite matcher, to revisit them when it changes: the parents of composite i
	// are parents[parent_offsets[i]] up to parents[parent_offsets[i + 1]]
	vector<idx_t> composite_of(matchers.size(), DConstants::INVALID_INDEX);
	for (idx_t i = 0; i < composites.size(); i++) {
		composite_of[composites[i].get().first_set.compute_index] = i;
	}
	vector<idx_t> parent_offsets(composites.size() + 1, 0);
	for (idx_t i = 0; i < composites.size(); i++) {
		ForEachChild(composites[i].get(), [&](Matcher &child) {
			auto child_index = composite_of[child.first_set.compute_index];
			if (child_index != DConstants::INVALID_INDEX) {
				parent_offsets[child_index + 1]++;
			}
		});
	}
	for (idx_t i = 0; i < composites.size(); i++) {
		parent_offsets[i + 1] += parent_offsets[i];
	}
	vector<idx_t> parents(parent_offsets.back());
	vector<idx_t> parent_fill(parent_offsets.begin(), parent_offsets.end() - 1);
	for (idx_t i = 0; i < composites.size(); i++) {
		ForEachChild(composites[i].get(), [&](Matcher &child) {
			auto child_index = composite_of[child.first_set.compute_index];
			if (child_index != DConstants::INVALID_INDEX) {
				parents[parent_fill[child_index]++] = i;
			}
		});
	}
	// composite matchers: least fixed point, the grammar is recursive
	ComputeFixedPoint(composites, parent_offsets, parents, vector<bool>(composites.size(), true), UpdateFirstSet);
	ComputeSecondSets(matchers, composites, parent_offsets, parents);
	auto lookahead_literals = storage.get() + matchers.size() * words;
	for (auto &entry : matchers) {
		auto &matcher = entry.get();
		auto &first = matcher.first_set;
		first.computed = true;
		first.compute_index = DConstants::INVALID_INDEX;
		if (!matcher.HasSecondTokenLookahead()) {
			first.second_literals = nullptr;
			first.ident_second_literals = nullptr;
			first.use_second = first.use_ident_second = false;
			continue;
		}
		memcpy(lookahead_literals, first.second_literals, 2 * words * sizeof(uint64_t));
		first.second_literals = lookahead_literals;
		first.ident_second_literals = lookahead_literals + words;
		lookahead_literals += 2 * words;
	}
	return storage;
}

bool MatcherFirstSet::MightMatch(MatchState &state) const {
	if (!computed || any || nullable) {
		return true;
	}
	auto token = state.token_iterator.Current();
	if (!token) {
		return false;
	}
	if (token->token_class & class_mask) {
		return true;
	}
	auto literal_id = state.token_iterator.CurrentLiteralInfo(*table).LiteralId();
	return literal_id && HasLiteral(literal_id);
}

//! Whether a token is in a token set given by its class mask and literal bitset
static bool InTokenSet(const MatcherToken &token, LiteralInfo literal, uint8_t class_mask, const uint64_t *literals,
                       idx_t words) {
	if (token.token_class & class_mask) {
		return true;
	}
	auto literal_id = literal.LiteralId();
	auto word = literal_id / 64;
	return literal_id && word < words && (literals[word] >> (literal_id % 64)) & 1;
}

bool MatcherFirstSet::MightMatchSecond(MatchState &state) const {
	if (!computed || (!use_second && !use_ident_second)) {
		return true;
	}
	auto token = state.token_iterator.Current();
	if (!token) {
		return true;
	}
	// a skipped attempt would have consumed the current token, which is only reproducible for the furthest-position
	// bookkeeping when the token is certainly consumed: an exact literal, an operator of a class some starting
	// matcher accepts entirely, or a plain identifier that is no keyword (then only identifier matchers consume it)
	auto literal_id = state.token_iterator.CurrentLiteralInfo(*table).LiteralId();
	bool identifier = false;
	if (literal_id && HasLiteral(literal_id)) {
		// consumed by a keyword matcher for exactly this literal
	} else if (token->token_class & exact_class_mask) {
		// consumed by an operator matcher that accepts its whole class
	} else if (!literal_id && (class_mask & MatcherTokenClass::WORD) &&
	           IdentifierMatcher::IsIdentifierToken(token->text)) {
		identifier = true;
	} else {
		return true;
	}
	if (identifier ? !use_ident_second : !use_second) {
		return true;
	}
	auto next = state.token_iterator.Next();
	if (next) {
		auto next_literal = state.token_iterator.NextLiteralInfo(*table);
		bool fits = identifier
		                ? InTokenSet(*next, next_literal, ident_second_class_mask, ident_second_literals, literal_words)
		                : InTokenSet(*next, next_literal, second_class_mask, second_literals, literal_words);
		if (fits) {
			return true;
		}
	}
	// the skipped attempt would have consumed the current token before failing on the next one
	auto position = state.token_iterator.Position();
	if (state.context.max_token_index < position + 1) {
		state.context.max_token_index = position + 1;
	}
	return false;
}

} // namespace duckdb
