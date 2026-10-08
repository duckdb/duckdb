#include "duckdb/parser/peg/matcher.hpp"
#include "duckdb/parser/peg/matcher/list.hpp"
#include "duckdb/common/reference_map.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// FIRST sets
//===--------------------------------------------------------------------===//
bool MatcherFirstSet::Merge(const MatcherFirstSet &other) {
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

void MatcherFirstSet::AddLiteral(idx_t literal_id) {
	auto word = literal_id / 64;
	if (word >= literals.size()) {
		literals.resize(word + 1, 0);
	}
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
static bool MergeTokenSet(bool &any, uint8_t &mask, vector<uint64_t> &literals, bool other_any, uint8_t other_mask,
                          const vector<uint64_t> &other_literals) {
	bool changed = false;
	if (other_any && !any) {
		any = changed = true;
	}
	if ((mask | other_mask) != mask) {
		mask |= other_mask;
		changed = true;
	}
	if (other_literals.size() > literals.size()) {
		literals.resize(other_literals.size(), 0);
	}
	for (idx_t i = 0; i < other_literals.size(); i++) {
		auto merged = literals[i] | other_literals[i];
		if (merged != literals[i]) {
			literals[i] = merged;
			changed = true;
		}
	}
	return changed;
}

//! Merge a FIRST set into the identifier-start second-token set
static bool MergeIntoIdentSecond(MatcherFirstSet &target, const MatcherFirstSet &first) {
	return MergeTokenSet(target.ident_second_any, target.ident_second_class_mask, target.ident_second_literals,
	                     first.any, first.class_mask, first.literals);
}

//! Merge another matcher's identifier-start second-token set into target
static bool MergeIdentSecond(MatcherFirstSet &target, const MatcherFirstSet &other) {
	return MergeTokenSet(target.ident_second_any, target.ident_second_class_mask, target.ident_second_literals,
	                     other.ident_second_any, other.ident_second_class_mask, other.ident_second_literals);
}

//! Merge a FIRST set into a second-token set, returns whether anything changed
static bool MergeIntoSecond(MatcherFirstSet &target, const MatcherFirstSet &first) {
	bool changed = false;
	if (first.any && !target.second_any) {
		target.second_any = changed = true;
	}
	if ((target.second_class_mask | first.class_mask) != target.second_class_mask) {
		target.second_class_mask |= first.class_mask;
		changed = true;
	}
	if (first.literals.size() > target.second_literals.size()) {
		target.second_literals.resize(first.literals.size(), 0);
	}
	for (idx_t i = 0; i < first.literals.size(); i++) {
		auto merged = target.second_literals[i] | first.literals[i];
		if (merged != target.second_literals[i]) {
			target.second_literals[i] = merged;
			changed = true;
		}
	}
	return changed;
}

//! Merge another matcher's second-token set into target, returns whether anything changed
static bool MergeSecond(MatcherFirstSet &target, const MatcherFirstSet &other) {
	bool changed = false;
	if (other.second_any && !target.second_any) {
		target.second_any = changed = true;
	}
	if ((target.second_class_mask | other.second_class_mask) != target.second_class_mask) {
		target.second_class_mask |= other.second_class_mask;
		changed = true;
	}
	if (other.second_literals.size() > target.second_literals.size()) {
		target.second_literals.resize(other.second_literals.size(), 0);
	}
	for (idx_t i = 0; i < other.second_literals.size(); i++) {
		auto merged = target.second_literals[i] | other.second_literals[i];
		if (merged != target.second_literals[i]) {
			target.second_literals[i] = merged;
			changed = true;
		}
	}
	return changed;
}

static bool SetFlag(bool &flag, bool value) {
	if (value && !flag) {
		flag = true;
		return true;
	}
	return false;
}

//! Least fixed point over the grammar; a superset of the tokens any atomic matcher can consume second on any path
static void ComputeSecondSets(vector<reference<Matcher>> &matchers) {
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
	bool changed = true;
	while (changed) {
		changed = false;
		for (auto &entry : matchers) {
			auto &matcher = entry.get();
			if (!IsComposite(matcher)) {
				continue;
			}
			auto &target = matcher.first_set;
			switch (matcher.Type()) {
			case MatcherType::LIST: {
				bool nullable = true;
				bool one = false;
				bool multi = false;
				bool ident_one = false;
				bool ident_multi = false;
				for (auto &child_ref : matcher.Cast<ListMatcher>().matchers) {
					auto &child = child_ref.get().first_set;
					// after a one-token prefix, the first token of the next element is the second token
					if (one) {
						changed |= MergeIntoSecond(target, child);
					}
					if (ident_one) {
						changed |= MergeIntoIdentSecond(target, child);
					}
					if (nullable) {
						changed |= MergeSecond(target, child);
						changed |= MergeIdentSecond(target, child);
					}
					bool new_one = (one && child.nullable) || (nullable && child.can_one);
					bool new_multi =
					    multi || (one && (child.can_one || child.can_multi)) || (nullable && child.can_multi);
					bool new_ident_one = (ident_one && child.nullable) || (nullable && child.ident_can_one);
					bool new_ident_multi = ident_multi || (ident_one && (child.can_one || child.can_multi)) ||
					                       (nullable && child.ident_can_multi);
					nullable = nullable && child.nullable;
					one = new_one;
					multi = new_multi;
					ident_one = new_ident_one;
					ident_multi = new_ident_multi;
				}
				changed |= SetFlag(target.can_one, one);
				changed |= SetFlag(target.can_multi, multi);
				changed |= SetFlag(target.ident_can_one, ident_one);
				changed |= SetFlag(target.ident_can_multi, ident_multi);
				break;
			}
			case MatcherType::CHOICE:
				for (auto &child_ref : matcher.Cast<ChoiceMatcher>().matchers) {
					auto &child = child_ref.get().first_set;
					changed |= MergeSecond(target, child);
					changed |= SetFlag(target.can_one, child.can_one);
					changed |= SetFlag(target.can_multi, child.can_multi);
					changed |= MergeIdentSecond(target, child);
					changed |= SetFlag(target.ident_can_one, child.ident_can_one);
					changed |= SetFlag(target.ident_can_multi, child.ident_can_multi);
				}
				break;
			case MatcherType::OPTIONAL: {
				auto &child = matcher.Cast<OptionalMatcher>().GetChildMatcher().first_set;
				changed |= MergeSecond(target, child);
				changed |= SetFlag(target.can_one, child.can_one);
				changed |= SetFlag(target.can_multi, child.can_multi);
				changed |= MergeIdentSecond(target, child);
				changed |= SetFlag(target.ident_can_one, child.ident_can_one);
				changed |= SetFlag(target.ident_can_multi, child.ident_can_multi);
				break;
			}
			case MatcherType::REPEAT: {
				auto &child = matcher.Cast<RepeatMatcher>().GetChildMatcher().first_set;
				changed |= MergeSecond(target, child);
				if (child.can_one) {
					// a one-token repetition followed by another repetition
					changed |= MergeIntoSecond(target, child);
				}
				changed |= SetFlag(target.can_one, child.can_one);
				changed |= SetFlag(target.can_multi, child.can_multi || child.can_one);
				changed |= MergeIdentSecond(target, child);
				if (child.ident_can_one) {
					changed |= MergeIntoIdentSecond(target, child);
				}
				changed |= SetFlag(target.ident_can_one, child.ident_can_one);
				changed |= SetFlag(target.ident_can_multi, child.ident_can_multi || child.ident_can_one);
				break;
			}
			default:
				break;
			}
		}
	}
	for (auto &entry : matchers) {
		auto &first = entry.get().first_set;
		first.use_second = !first.nullable && !first.any && !first.can_one && !first.second_any;
		first.use_ident_second = !first.nullable && !first.any && !first.ident_can_one && !first.ident_second_any;
	}
}

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
			static_cast<const AtomicMatcher &>(matcher).InitializeFirstSet(first, table);
		} else {
			// custom and derived matchers: no information
			first.any = true;
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
	ComputeSecondSets(matchers);
	for (auto &entry : matchers) {
		entry.get().first_set.computed = true;
	}
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

//! A word or a double-quoted identifier, consumed by every identifier matcher when it is not a keyword
static bool IsPlainIdentifierToken(const string &text) {
	if (text.empty()) {
		return false;
	}
	auto c = static_cast<unsigned char>(text[0]);
	if (c == '"') {
		return text.size() > 1 && text.back() == '"';
	}
	return isalpha(c) || c == '_';
}

//! Whether a token is in a token set given by its class mask and literal bitset
static bool InTokenSet(const MatcherToken &token, LiteralInfo literal, uint8_t class_mask,
                       const vector<uint64_t> &literals) {
	if (token.token_class & class_mask) {
		return true;
	}
	auto literal_id = literal.LiteralId();
	auto word = literal_id / 64;
	return literal_id && word < literals.size() && (literals[word] >> (literal_id % 64)) & 1;
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
	} else if (!literal_id && (class_mask & MatcherTokenClass::WORD) && IsPlainIdentifierToken(token->text)) {
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
		bool fits = identifier ? InTokenSet(*next, next_literal, ident_second_class_mask, ident_second_literals)
		                       : InTokenSet(*next, next_literal, second_class_mask, second_literals);
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
