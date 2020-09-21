#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/function/scalar/string_functions.hpp"

using namespace std;

namespace duckdb {

template <char PERCENTAGE, char UNDERSCORE>
bool templated_like_operator(const char *s, const char *pattern, const char *escape) {
	const char *t, *p;

	t = s;
	for (p = pattern; *p && *t; p++) {
		if (escape && *p == *escape) {
			p++;
			if (*p != *t) {
				return false;
			}
			t++;
		} else if (*p == UNDERSCORE) {
			t++;
		} else if (*p == PERCENTAGE) {
			p++;
			while (*p == PERCENTAGE) {
				p++;
			}
			if (*p == 0) {
				return true; /* tail is acceptable */
			}
			for (; *p && *t; t++) {
				if (templated_like_operator<PERCENTAGE, UNDERSCORE>(t, p, escape)) {
					return true;
				}
			}
			if (*p == 0 && *t == 0) {
				return true;
			}
			return false;
		} else if (*p == *t) {
			t++;
		} else {
			return false;
		}
	}
	if (*p == PERCENTAGE && *(p + 1) == 0) {
		return true;
	}
	return *t == 0 && *p == 0;
}

bool like_operator(const char *s, const char *pattern, const char *escape) {
	return templated_like_operator<'%', '_'>(s, pattern, escape);
}

bool LikeFun::Glob(const char *s, const char *pattern, const char *escape) {
	return templated_like_operator<'*', '?'>(s, pattern, escape);
}

struct LikeEscapeOperator {
	template <class TA, class TB, class TC> static inline bool Operation(TA str, TB pattern, TC escape) {
		// Only one escape character should be allowed
		if (escape.GetSize() > 1) {
			throw SyntaxException("Invalid escape string. Escape string must be empty or one character.");
		}
		return like_operator(str.GetData(), pattern.GetData(), escape.GetData());
	}
};

struct NotLikeEscapeOperator {
	template <class TA, class TB, class TC> static inline bool Operation(TA str, TB pattern, TC escape) {
		return !LikeEscapeOperator::Operation(str, pattern, escape);
	}
};

struct LikeOperator {
	template <class TA, class TB, class TR> static inline TR Operation(TA str, TB pattern) {
		return like_operator(str.GetData(), pattern.GetData(), nullptr);
	}
};

struct NotLikeOperator {
	template <class TA, class TB, class TR> static inline TR Operation(TA str, TB pattern) {
		return !like_operator(str.GetData(), pattern.GetData(), nullptr);
	}
};

struct GlobOperator {
	template <class TA, class TB, class TR> static inline TR Operation(TA str, TB pattern) {
		return LikeFun::Glob(str.GetData(), pattern.GetData(), nullptr);
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
