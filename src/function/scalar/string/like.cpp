#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/function/scalar/string_functions.hpp"

using namespace std;

namespace duckdb {

template <char PERCENTAGE, char UNDERSCORE>
bool templated_like_operator(const char *sdata, idx_t slen, const char *pdata, idx_t plen, char escape) {
	idx_t pidx = 0;
	idx_t sidx = 0;
	for(; pidx < plen && sidx < slen; pidx++) {
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
				if (templated_like_operator<PERCENTAGE, UNDERSCORE>(sdata + sidx, slen - sidx, pdata + pidx, plen - pidx, escape)) {
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
	while(pidx < plen && pdata[pidx] == PERCENTAGE) {
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

bool LikeFun::Glob(const char *s, idx_t slen, const char *pattern, idx_t plen, char escape) {
	return templated_like_operator<'*', '?'>(s, slen, pattern, plen, escape);
}

struct LikeEscapeOperator {
	template <class TA, class TB, class TC> static inline bool Operation(TA str, TB pattern, TC escape) {
		// Only one escape character should be allowed
		if (escape.GetSize() > 1) {
			throw SyntaxException("Invalid escape string. Escape string must be empty or one character.");
		}
		char escape_char = escape.GetSize() == 0 ? '\0' : *escape.GetDataUnsafe();
		return like_operator(str.GetDataUnsafe(), str.GetSize(), pattern.GetDataUnsafe(), pattern.GetSize(), escape_char);
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
