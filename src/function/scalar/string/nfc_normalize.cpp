#include "duckdb/function/scalar/string_functions.hpp"

#include "utf8proc_wrapper.hpp"

namespace duckdb {

struct NFCNormalizeOperator {
	template <class INPUT_TYPE, class RESULT_TYPE>
	static RESULT_TYPE Operation(INPUT_TYPE input, Vector &result) {
		auto input_data = input.GetData();
		auto input_length = input.GetSize();
		if (StripAccentsFun::IsAscii(input_data, input_length)) {
			return input;
		}
		auto normalized_str = Utf8Proc::Normalize(input_data, input_length);
		D_ASSERT(normalized_str);
		auto result_str = StringVector::AddString(result, normalized_str);
		free(normalized_str);
		return result_str;
	}
};

static void NFCNormalizeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.ColumnCount() == 1);

	UnaryExecutor::ExecuteString<string_t, string_t, NFCNormalizeOperator>(args.data[0], result, args.size());
	StringVector::AddHeapReference(result, args.data[0]);
}

ScalarFunction NFCNormalizeFun::GetFunction() {
	return ScalarFunction("nfc_normalize", {LogicalType::VARCHAR}, LogicalType::VARCHAR, NFCNormalizeFunction);
}

void NFCNormalizeFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(NFCNormalizeFun::GetFunction());
}

class NFCNormalizeFun::NFCNormalizeTransform {
public:
	static char *Operator(const char *data, size_t len) {
		if (StripAccentsFun::IsAscii(data, len)) {
			return nullptr;
		}
		// Normalize() will return nfc transformed string with '\0' as
		// end character.
		return reinterpret_cast<char *>(Utf8Proc::Normalize(data, len));
	}
};

template <bool INVERT>
static void NFCNormalizeLikeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.ColumnCount() == 2);
	BinaryExecutor::Execute<string_t, string_t, bool>(
	    args.data[0], args.data[1], result, args.size(), [](string_t input, string_t pattern) {
		    string_t escape("");
		    return LikeFun::LikeWithCollation<'%', '_', NFCNormalizeFun::NFCNormalizeTransform>(input, pattern,
		                                                                                        escape) ^
		           INVERT;
	    });
}

template <bool INVERT>
static void NFCNormalizeLikeEscapeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.ColumnCount() == 3);
	TernaryExecutor::Execute<string_t, string_t, string_t, bool>(
	    args.data[0], args.data[1], args.data[2], result, args.size(),
	    [](string_t input, string_t pattern, string_t escape) {
		    return LikeFun::LikeWithCollation<'%', '_', NFCNormalizeFun::NFCNormalizeTransform>(input, pattern,
		                                                                                        escape) ^
		           INVERT;
	    });
}

template <bool INVERT, bool ESCAPE>
ScalarFunction NFCNormalizeFun::GetLikeFunction() {
	string name = "nfc~" + LikeFun::GetLikeFunctionName(INVERT, ESCAPE);
	if (ESCAPE) {
		return ScalarFunction(name.c_str(), {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
		                      LogicalType::BOOLEAN, NFCNormalizeLikeEscapeFunction<INVERT>);
	} else {
		return ScalarFunction(name.c_str(), {LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::BOOLEAN,
		                      NFCNormalizeLikeFunction<INVERT>);
	}
}

} // namespace duckdb
