#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/function/scalar/string_functions.hpp"

using namespace std;

namespace duckdb {

template <char PERCENTAGE, char UNDERSCORE>
bool templated_like_operator(const char *sdata, idx_t slen, const char *pdata, idx_t plen, char escape) {
	idx_t pidx = 0;
	idx_t sidx = 0;
	for (; pidx < plen && sidx < slen; pidx++) {
		char pchar = pdata[pidx];
		char schar = sdata[sidx];
		if (pchar == escape) {
			pidx++;
			if (pidx == plen) {
				throw SyntaxException("Like pattern must not end with escape character!");
			}
			if (pdata[pidx] != schar) {
				return false;
			}
			sidx++;
		} else if (pchar == UNDERSCORE) {
			sidx++;
		} else if (pchar == PERCENTAGE) {
			pidx++;
			while (pidx < plen && pdata[pidx] == PERCENTAGE) {
				pidx++;
			}
			if (pidx == plen) {
				return true; /* tail is acceptable */
			}
			for (; sidx < slen; sidx++) {
				if (templated_like_operator<PERCENTAGE, UNDERSCORE>(sdata + sidx, slen - sidx, pdata + pidx,
				                                                    plen - pidx, escape)) {
					return true;
				}
			}
			return false;
		} else if (pchar == schar) {
			sidx++;
		} else {
			return false;
		}
	}
	while (pidx < plen && pdata[pidx] == PERCENTAGE) {
		pidx++;
	}
	return pidx == plen && sidx == slen;
}

bool like_operator(const char *s, idx_t slen, const char *pattern, idx_t plen, char escape) {
	return templated_like_operator<'%', '_'>(s, slen, pattern, plen, escape);
}

bool like_operator(string_t &s, string_t &pat, char escape = '\0') {
	return like_operator(s.GetDataUnsafe(), s.GetSize(), pat.GetDataUnsafe(), pat.GetSize(), escape);
}

bool LikeFun::Glob(const char *string, idx_t slen, const char *pattern, idx_t plen) {
	idx_t sidx = 0;
	idx_t pidx = 0;
main_loop : {
	// main matching loop
	while (sidx < slen && pidx < plen) {
		char s = string[sidx];
		char p = pattern[pidx];
		switch (p) {
		case '*': {
			// asterisk: match any set of characters
			// skip any subsequent asterisks
			pidx++;
			while (pidx < plen && pattern[pidx] == '*') {
				pidx++;
			}
			// if the asterisk is the last character, the pattern always matches
			if (pidx == plen) {
				return true;
			}
			// recursively match the remainder of the pattern
			for (; sidx < slen; sidx++) {
				if (LikeFun::Glob(string + sidx, slen - sidx, pattern + pidx, plen - pidx)) {
					return true;
				}
			}
			return false;
		}
		case '?':
			// wildcard: matches anything but null
			sidx++;
			pidx++;
			break;
		case '[':
			pidx++;
			goto parse_bracket;
		case '\\':
			// escape character, next character needs to match literally
			pidx++;
			// check that we still have a character remaining
			if (pidx == plen) {
				return false;
			}
			p = pattern[pidx];
			if (s != p) {
				return false;
			}
			sidx++;
			pidx++;
			break;
		default:
			// not a control character: characters need to match literally
			if (s != p) {
				return false;
			}
			sidx++;
			pidx++;
			break;
		}
	}
	while (pidx < plen && pattern[pidx] == '*') {
		pidx++;
	}
	// we are finished only if we have consumed the full pattern
	return pidx == plen && sidx == slen;
}
parse_bracket : {
	// inside a bracket
	if (pidx == plen) {
		return false;
	}
	// check the first character
	// if it is an exclamation mark we need to invert our logic
	char p = pattern[pidx];
	char s = string[sidx];
	bool invert = false;
	if (p == '!') {
		invert = true;
		pidx++;
	}
	bool found_match = invert;
	idx_t start_pos = pidx;
	bool found_closing_bracket = false;
	// now check the remainder of the pattern
	while (pidx < plen) {
		p = pattern[pidx];
		// if the first character is a closing bracket, we match it literally
		// otherwise it indicates an end of bracket
		if (p == ']' && pidx > start_pos) {
			// end of bracket found: we are done
			found_closing_bracket = true;
			pidx++;
			break;
		}
		// we either match a range (a-b) or a single character (a)
		// check if the next character is a dash
		if (pidx + 1 == plen) {
			// no next character!
			break;
		}
		bool matches;
		if (pattern[pidx + 1] == '-') {
			// range! find the next character in the range
			if (pidx + 2 == plen) {
				break;
			}
			char next_char = pattern[pidx + 2];
			// check if the current character is within the range
			matches = s >= p && s <= next_char;
			// shift the pattern forward past the range
			pidx += 3;
		} else {
			// no range! perform a direct match
			matches = p == s;
			// shift the pattern forward past the character
			pidx++;
		}
		if (found_match == invert && matches) {
			// found a match! set the found_matches flag
			// we keep on pattern matching after this until we reach the end bracket
			// however, we don't need to update the found_match flag anymore
			found_match = !invert;
		}
	}
	if (!found_closing_bracket) {
		// no end of bracket: invalid pattern
		return false;
	}
	if (!found_match) {
		// did not match the bracket: return false;
		return false;
	}
	// finished the bracket matching: move forward
	sidx++;
	goto main_loop;
}
}

struct LikeEscapeOperator {
	template <class TA, class TB, class TC> static inline bool Operation(TA str, TB pattern, TC escape) {
		// Only one escape character should be allowed
		if (escape.GetSize() > 1) {
			throw SyntaxException("Invalid escape string. Escape string must be empty or one character.");
		}
		char escape_char = escape.GetSize() == 0 ? '\0' : *escape.GetDataUnsafe();
		return like_operator(str.GetDataUnsafe(), str.GetSize(), pattern.GetDataUnsafe(), pattern.GetSize(),
		                     escape_char);
	}
};

struct NotLikeEscapeOperator {
	template <class TA, class TB, class TC> static inline bool Operation(TA str, TB pattern, TC escape) {
		return !LikeEscapeOperator::Operation(str, pattern, escape);
	}
};

struct LikeOperator {
	template <class TA, class TB, class TR> static inline TR Operation(TA str, TB pattern) {
		return like_operator(str, pattern);
	}
};

struct NotLikeOperator {
	template <class TA, class TB, class TR> static inline TR Operation(TA str, TB pattern) {
		return !like_operator(str, pattern);
	}
};

struct GlobOperator {
	template <class TA, class TB, class TR> static inline TR Operation(TA str, TB pattern) {
		return LikeFun::Glob(str.GetDataUnsafe(), str.GetSize(), pattern.GetDataUnsafe(), pattern.GetSize());
	}
};

// This can be moved to the scalar_function class
template <typename Func> static void like_escape_function(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &str = args.data[0];
	auto &pattern = args.data[1];
	auto &escape = args.data[2];

	TernaryExecutor::Execute<string_t, string_t, string_t, bool>(
	    str, pattern, escape, result, args.size(), Func::template Operation<string_t, string_t, string_t>);
}

void LikeFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(ScalarFunction("~~", {LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::BOOLEAN,
	                               ScalarFunction::BinaryFunction<string_t, string_t, bool, LikeOperator, true>));
	set.AddFunction(ScalarFunction("!~~", {LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::BOOLEAN,
	                               ScalarFunction::BinaryFunction<string_t, string_t, bool, NotLikeOperator, true>));
	// glob function
	set.AddFunction(ScalarFunction("~~~", {LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::BOOLEAN,
	                               ScalarFunction::BinaryFunction<string_t, string_t, bool, GlobOperator, true>));
}

void LikeEscapeFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction({"like_escape"}, ScalarFunction({LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
	                                                LogicalType::BOOLEAN, like_escape_function<LikeEscapeOperator>));
	set.AddFunction({"not_like_escape"},
	                ScalarFunction({LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
	                               LogicalType::BOOLEAN, like_escape_function<NotLikeEscapeOperator>));
}
} // namespace duckdb
