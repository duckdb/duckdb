#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/function/scalar/string_functions.hpp"

using namespace std;

namespace duckdb {

static bool like_operator(const char *s, const char *pattern, const char *escape);

struct LikeEscapeOperator {
	template <class TA, class TB, class TR> static inline TR Operation(TA left, TB right) {
		auto escape = right.GetData();
		return like_operator(left.GetData(), right.GetData(), escape);
	}
};

struct NotLikeEscapeOperator {
	template <class TA, class TB, class TR> static inline TR Operation(TA left, TB right) {
		auto escape = right.GetData();
		return !like_operator(left.GetData(), right.GetData(), escape);
	}
};

struct LikeOperator {
	template <class TA, class TB, class TR> static inline TR Operation(TA left, TB right) {
		return like_operator(left.GetData(), right.GetData(), nullptr);
	}
};

struct NotLikeOperator {
	template <class TA, class TB, class TR> static inline TR Operation(TA left, TB right) {
		return !like_operator(left.GetData(), right.GetData(), nullptr);
	}
};

bool like_operator(const char *s, const char *pattern, const char *escape) {
	const char *t, *p;

	t = s;
	for (p = pattern; *p && *t; p++) {
		if (escape && *t == *escape) {
			t++;
			if (*p != *t) {
				return false;
			}
		} else if (*p == '_') {
			t++;
		} else if (*p == '%') {
			p++;
			while (*p == '%') {
				p++;
			}
			if (*p == 0) {
				return true; /* tail is acceptable */
			}
			for (; *p && *t; t++) {
				if (like_operator(t, p, escape)) {
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
	if (*p == '%' && *(p + 1) == 0) {
		return true;
	}
	return *t == 0 && *p == 0;
}

void LikeFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(ScalarFunction("~~", {SQLType::VARCHAR, SQLType::VARCHAR}, SQLType::BOOLEAN,
	                               ScalarFunction::BinaryFunction<string_t, string_t, bool, LikeOperator, true>));
	set.AddFunction(ScalarFunction("!~~", {SQLType::VARCHAR, SQLType::VARCHAR}, SQLType::BOOLEAN,
	                               ScalarFunction::BinaryFunction<string_t, string_t, bool, NotLikeOperator, true>));
}

void LikeEscapeFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(ScalarFunction("like_escape", {SQLType::VARCHAR, SQLType::VARCHAR}, SQLType::BOOLEAN,
	                               ScalarFunction::BinaryFunction<string_t, string_t, bool, LikeEscapeOperator, true>));
	set.AddFunction(
	    ScalarFunction("!like_escape", {SQLType::VARCHAR, SQLType::VARCHAR}, SQLType::BOOLEAN,
	                   ScalarFunction::BinaryFunction<string_t, string_t, bool, NotLikeEscapeOperator, true>));
}

} // namespace duckdb
