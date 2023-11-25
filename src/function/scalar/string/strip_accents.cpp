#include "duckdb/function/scalar/string_functions.hpp"

#include "utf8proc.hpp"

namespace duckdb {

bool StripAccentsFun::IsAscii(const char *input, idx_t n) {
	for (idx_t i = 0; i < n; i++) {
		if (input[i] & 0x80) {
			// non-ascii character
			return false;
		}
	}
	return true;
}

struct StripAccentsOperator {
	template <class INPUT_TYPE, class RESULT_TYPE>
	static RESULT_TYPE Operation(INPUT_TYPE input, Vector &result) {
		if (StripAccentsFun::IsAscii(input.GetData(), input.GetSize())) {
			return input;
		}

		// non-ascii, perform collation
		auto stripped = utf8proc_remove_accents((const utf8proc_uint8_t *)input.GetData(), input.GetSize());
		auto result_str = StringVector::AddString(result, const_char_ptr_cast(stripped));
		free(stripped);
		return result_str;
	}
};

static void StripAccentsFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.ColumnCount() == 1);

	UnaryExecutor::ExecuteString<string_t, string_t, StripAccentsOperator>(args.data[0], result, args.size());
	StringVector::AddHeapReference(result, args.data[0]);
}

ScalarFunction StripAccentsFun::GetFunction() {
	return ScalarFunction("strip_accents", {LogicalType::VARCHAR}, LogicalType::VARCHAR, StripAccentsFunction);
}

class StripAccentsFun::StripAccentsTransform {
public:
	static char *Operator(const char *data, size_t len) {
		if (StripAccentsFun::IsAscii(data, len)) {
			return nullptr;
		}
		// utf8proc_remove_accents() will return the strip-accents transformed string with
		// '\0' as end character.
		return reinterpret_cast<char *>(utf8proc_remove_accents(reinterpret_cast<const utf8proc_uint8_t *>(data), len));
	}
};

template <bool INVERT>
static void StripAccentsLikeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.ColumnCount() == 2);
	BinaryExecutor::Execute<string_t, string_t, bool>(
	    args.data[0], args.data[1], result, args.size(), [](string_t input, string_t pattern) {
		    string_t escape("");
		    return LikeFun::LikeWithCollation<'%', '_', StripAccentsFun::StripAccentsTransform>(input, pattern,
		                                                                                        escape) ^
		           INVERT;
	    });
}

template <bool INVERT>
static void StripAccentsLikeEscapeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.ColumnCount() == 3);
	TernaryExecutor::Execute<string_t, string_t, string_t, bool>(
	    args.data[0], args.data[1], args.data[2], result, args.size(),
	    [](string_t input, string_t pattern, string_t escape) {
		    return LikeFun::LikeWithCollation<'%', '_', StripAccentsFun::StripAccentsTransform>(input, pattern,
		                                                                                        escape) ^
		           INVERT;
	    });
}

template <bool INVERT, bool ESCAPE>
ScalarFunction StripAccentsFun::GetLikeFunction() {
	string name = "noaccent~" + LikeFun::GetLikeFunctionName(INVERT, ESCAPE);
	if (ESCAPE) {
		return ScalarFunction(name.c_str(), {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
		                      LogicalType::BOOLEAN, StripAccentsLikeEscapeFunction<INVERT>);
	} else {
		return ScalarFunction(name.c_str(), {LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::BOOLEAN,
		                      StripAccentsLikeFunction<INVERT>);
	}
}

void StripAccentsFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(StripAccentsFun::GetFunction());
}

} // namespace duckdb
