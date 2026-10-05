#include "duckdb/parser/peg/matcher.hpp"
#include "duckdb/parser/peg/matcher/list.hpp"
#include "duckdb/parser/peg/tokenizer/tokenizer.hpp"
#include "duckdb/common/reference_map.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Token classes
//===--------------------------------------------------------------------===//
// Each class is a superset of the tokens the corresponding atomic matchers accept, so that a matcher whose FIRST set
// misses a token's class (and literal) can never match it.

//! Identifiers (plain, quoted or single-quoted) and keywords
static bool IsWordToken(const string &text) {
	if (text.empty()) {
		return false;
	}
	auto c = static_cast<unsigned char>(text[0]);
	return c >= 0x80 || isalpha(c) || c == '_' || c == '"' || c == '\'' || c == '$' || c == '`';
}

//! OperatorMatcher and ArithmeticOperatorMatcher only accept tokens that consist of operator characters
static bool IsOperatorToken(const string &text) {
	if (text.empty()) {
		return false;
	}
	for (auto c : text) {
		if (!Tokenizer::CharacterIsOperator(c)) {
			return false;
		}
	}
	return true;
}

//! 'x', E'x' / X'x' / B'x' / N'x', $$x$$
static bool IsStringToken(const string &text) {
	if (text.empty()) {
		return false;
	}
	return text[0] == '\'' || text[0] == '$' || (text.size() > 1 && text[1] == '\'');
}

uint8_t ComputeMatcherTokenClass(const string &text) {
	uint8_t result = 0;
	if (IsWordToken(text)) {
		result |= MatcherTokenClass::WORD;
	}
	if (IsOperatorToken(text)) {
		result |= MatcherTokenClass::OPERATOR;
		if (!OperatorMatcher::HasSpecialPrecedence(text)) {
			result |= MatcherTokenClass::GENERIC_OPERATOR;
		}
	}
	if (IsStringToken(text)) {
		result |= MatcherTokenClass::STRING;
	}
	if (!text.empty() && Tokenizer::CharacterIsInitialNumber(text[0])) {
		result |= MatcherTokenClass::NUMBER;
	}
	return result;
}

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

static void CollectChildren(Matcher &matcher, vector<reference<Matcher>> &children) {
	switch (matcher.Type()) {
	case MatcherType::LIST:
		for (auto &child : matcher.Cast<ListMatcher>().matchers) {
			children.push_back(child);
		}
		break;
	case MatcherType::CHOICE:
		for (auto &child : matcher.Cast<ChoiceMatcher>().matchers) {
			children.push_back(child);
		}
		break;
	case MatcherType::OPTIONAL:
		children.push_back(matcher.Cast<OptionalMatcher>().GetChildMatcher());
		break;
	case MatcherType::REPEAT:
		children.push_back(matcher.Cast<RepeatMatcher>().GetChildMatcher());
		break;
	default:
		break;
	}
}

static bool IsComposite(MatcherType type) {
	return type == MatcherType::LIST || type == MatcherType::CHOICE || type == MatcherType::OPTIONAL ||
	       type == MatcherType::REPEAT;
}

void ComputeFirstSets(Matcher &root, const GrammarLiteralTable &table) {
	// collect the reachable matchers
	vector<reference<Matcher>> matchers;
	reference_set_t<Matcher> seen;
	vector<reference<Matcher>> pending {root};
	while (!pending.empty()) {
		auto &matcher = pending.back().get();
		pending.pop_back();
		if (!seen.insert(matcher).second) {
			continue;
		}
		matchers.push_back(matcher);
		CollectChildren(matcher, pending);
	}
	// atomic matchers
	for (auto &entry : matchers) {
		auto &matcher = entry.get();
		auto &first = matcher.first_set;
		first = MatcherFirstSet();
		first.table = &table;
		switch (matcher.Type()) {
		case MatcherType::KEYWORD: {
			auto literal = matcher.Cast<KeywordMatcher>().GetDispatchLiteral(table);
			if (literal.IsValid()) {
				first.AddLiteral(literal.GetIndex());
			} else {
				first.any = true;
			}
			break;
		}
		case MatcherType::VARIABLE:
			first.class_mask = MatcherTokenClass::WORD;
			break;
		case MatcherType::OPERATOR: {
			auto op = dynamic_cast<const OperatorMatcher *>(&matcher);
			first.class_mask =
			    op && op->IsGenericPrecedence() ? MatcherTokenClass::GENERIC_OPERATOR : MatcherTokenClass::OPERATOR;
			break;
		}
		case MatcherType::STRING_LITERAL:
			first.class_mask = MatcherTokenClass::STRING;
			break;
		case MatcherType::NUMBER_LITERAL:
			first.class_mask = MatcherTokenClass::NUMBER;
			break;
		default:
			if (!IsComposite(matcher.Type())) {
				// end of input and custom matchers: no information
				first.any = true;
			}
			break;
		}
	}
	// composite matchers: least fixed point, the grammar is recursive
	bool changed = true;
	while (changed) {
		changed = false;
		for (auto &entry : matchers) {
			auto &matcher = entry.get();
			auto type = matcher.Type();
			if (!IsComposite(type)) {
				continue;
			}
			auto &first = matcher.first_set;
			vector<reference<Matcher>> children;
			CollectChildren(matcher, children);
			bool nullable;
			if (type == MatcherType::LIST) {
				// the first non-nullable element ends the FIRST set of a sequence
				nullable = true;
				for (auto &child : children) {
					changed |= first.Merge(child.get().first_set);
					if (!child.get().first_set.nullable) {
						nullable = false;
						break;
					}
				}
			} else if (type == MatcherType::CHOICE) {
				nullable = false;
				for (auto &child : children) {
					changed |= first.Merge(child.get().first_set);
					nullable = nullable || child.get().first_set.nullable;
				}
			} else {
				changed |= first.Merge(children[0].get().first_set);
				nullable = type == MatcherType::OPTIONAL || children[0].get().first_set.nullable;
			}
			if (nullable && !first.nullable) {
				first.nullable = true;
				changed = true;
			}
		}
	}
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

} // namespace duckdb
