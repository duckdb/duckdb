#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/function/scalar/string_functions.hpp"

using namespace std;

namespace duckdb {

static bool like_operator(const char *s, const char *pattern, const char *escape);

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
		;
	}
};

bool like_operator(const char *s, const char *pattern, const char *escape) {
	const char *t, *p;

	t = s;
	for (p = pattern; *p && *t; p++) {
		if (escape && *p == *escape) {
			p++;
			if (*p != *t) {
				return false;
			}
			t++;
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
} // namespace duckdb

// This can be moved to the scalar_function class
template <typename Func> static void like_escape_function(DataChunk &args, ExpressionState &state, Vector &result) {
	assert(args.column_count() == 3 && args.data[0].type == TypeId::VARCHAR && args.data[1].type == TypeId::VARCHAR &&
	       args.data[2].type == TypeId::VARCHAR);
	auto &str = args.data[0];
	auto &pattern = args.data[1];
	auto &escape = args.data[2];

	TernaryExecutor::Execute<string_t, string_t, string_t, bool>(
	    str, pattern, escape, result, args.size(), Func::template Operation<string_t, string_t, string_t>);
}

void LikeFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(ScalarFunction("~~", {SQLType::VARCHAR, SQLType::VARCHAR}, SQLType::BOOLEAN,
	                               ScalarFunction::BinaryFunction<string_t, string_t, bool, LikeOperator, true>));
	set.AddFunction(ScalarFunction("!~~", {SQLType::VARCHAR, SQLType::VARCHAR}, SQLType::BOOLEAN,
	                               ScalarFunction::BinaryFunction<string_t, string_t, bool, NotLikeOperator, true>));
}

void LikeEscapeFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction({"like_escape"}, ScalarFunction({SQLType::VARCHAR, SQLType::VARCHAR, SQLType::VARCHAR},
	                                                SQLType::BOOLEAN, like_escape_function<LikeEscapeOperator>));
	set.AddFunction({"not_like_escape"}, ScalarFunction({SQLType::VARCHAR, SQLType::VARCHAR, SQLType::VARCHAR},
	                                                    SQLType::BOOLEAN, like_escape_function<NotLikeEscapeOperator>));
}
} // namespace duckdb
