#include "duckdb/function/scalar/string_functions.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"

#include <string.h>

using namespace std;

namespace duckdb {

struct SpaceChar {
	static char Operation(char input) {
		return isspace(input);
	}
};

struct KeptChar {
	static char Operation(char input) {
		return false;
	}
};

template <class LTRIM, class RTRIM> static void trim_function(Vector &input, Vector &result, idx_t count) {
	assert(input.type == TypeId::VARCHAR);

	UnaryExecutor::Execute<string_t, string_t, true>(input, result, count, [&](string_t input) {
		auto input_data = input.GetData();
		auto input_length = input.GetSize();

		idx_t begin = 0;
		for (; begin < input_length; ++begin)
			if (!LTRIM::Operation(input_data[begin])) break;
			
		idx_t end = input_length;
		for (; begin < --end;)
			if (!RTRIM::Operation(input_data[end])) break;
		++end;

		auto target = StringVector::EmptyString(result, end - begin);
		auto output_data = target.GetData();
		for (idx_t i = begin; i < end; ++i)
			*output_data++ = input_data[i];
		*output_data++ = '\0';

		target.Finalize();
		return target;
	});
}

static void trim_ltrim_function(DataChunk &args, ExpressionState &state, Vector &result) {
	assert(args.column_count() == 1);
	trim_function<SpaceChar,KeptChar>(args.data[0], result, args.size());
}

static void trim_rtrim_function(DataChunk &args, ExpressionState &state, Vector &result) {
	assert(args.column_count() == 1);
	trim_function<KeptChar,SpaceChar>(args.data[0], result, args.size());
}

void LtrimFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(ScalarFunction("ltrim", {SQLType::VARCHAR}, SQLType::VARCHAR, trim_ltrim_function));
}

void RtrimFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(ScalarFunction("rtrim", {SQLType::VARCHAR}, SQLType::VARCHAR, trim_rtrim_function));
}

} // namespace duckdb
