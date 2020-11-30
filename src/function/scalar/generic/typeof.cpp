#include "duckdb/function/scalar/generic_functions.hpp"

using namespace std;

namespace duckdb {

static void typeof_function(DataChunk &args, ExpressionState &state, Vector &result) {
	Value v(args.data[0].type.ToString());
	result.Reference(v);
}

void TypeOfFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(ScalarFunction("typeof", {LogicalType::ANY}, LogicalType::VARCHAR,
	                                     typeof_function));
}

} // namespace duckdb
