#include "jaro_winkler.hpp"

#include "duckdb/function/scalar/string_functions.hpp"

namespace duckdb {

static inline double JaroScalarFunction(const string_t &s1, const string_t &s2) {
	return jaro_winkler::jaro_similarity(s1.GetDataUnsafe(), s1.GetDataUnsafe() + s1.GetSize(), s2.GetDataUnsafe(),
	                                     s2.GetDataUnsafe() + s2.GetSize());
}

static inline double JaroWinklerScalarFunction(const string_t &s1, const string_t &s2) {
	return jaro_winkler::jaro_winkler_similarity(s1.GetDataUnsafe(), s1.GetDataUnsafe() + s1.GetSize(),
	                                             s2.GetDataUnsafe(), s2.GetDataUnsafe() + s2.GetSize());
}

static void JaroFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	BinaryExecutor::Execute<string_t, string_t, double>(
	    args.data[0], args.data[1], result, args.size(),
	    [](const string_t &s1, const string_t &s2) { return JaroScalarFunction(s1, s2); });
}

static void JaroWinklerFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	BinaryExecutor::Execute<string_t, string_t, double>(
	    args.data[0], args.data[1], result, args.size(),
	    [](const string_t &s1, const string_t &s2) { return JaroWinklerScalarFunction(s1, s2); });
}

void JaroWinklerFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(ScalarFunction("jaro_similarity", {LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::DOUBLE,
	                               JaroFunction));
	set.AddFunction(ScalarFunction("jaro_winkler_similarity", {LogicalType::VARCHAR, LogicalType::VARCHAR},
	                               LogicalType::DOUBLE, JaroWinklerFunction));
}

} // namespace duckdb
