#include "jaro_winkler.hpp"

#include "duckdb/function/scalar/string_functions.hpp"

namespace duckdb {

static inline double JaroScalarFunction(const string_t &s1, const string_t &s2) {
	auto s1_begin = s1.GetDataUnsafe();
	auto s2_begin = s2.GetDataUnsafe();
	return duckdb_jaro_winkler::jaro_similarity(s1_begin, s1_begin + s1.GetSize(), s2_begin, s2_begin + s2.GetSize());
}

static inline double JaroWinklerScalarFunction(const string_t &s1, const string_t &s2) {
	auto s1_begin = s1.GetDataUnsafe();
	auto s2_begin = s2.GetDataUnsafe();
	return duckdb_jaro_winkler::jaro_winkler_similarity(s1_begin, s1_begin + s1.GetSize(), s2_begin,
	                                                    s2_begin + s2.GetSize());
}

template <class CACHED_SIMILARITY>
static void CachedFunction(Vector &constant, Vector &other, Vector &result, idx_t count) {
	auto val = constant.GetValue(0);
	if (val.IsNull()) {
		auto &result_validity = FlatVector::Validity(result);
		result_validity.SetAllInvalid(count);
		return;
	}

	auto str_val = StringValue::Get(val);
	auto cached = CACHED_SIMILARITY(str_val);
	UnaryExecutor::Execute<string_t, double>(other, result, count, [&](const string_t &other_str) {
		auto other_str_begin = other_str.GetDataUnsafe();
		return cached.similarity(other_str_begin, other_str_begin + other_str.GetSize());
	});
}

template <class CACHED_SIMILARITY, class SIMILARITY_FUNCTION = std::function<double(string_t, string_t)>>
static void TemplatedJaroWinklerFunction(DataChunk &args, Vector &result, SIMILARITY_FUNCTION fun) {
	bool arg0_constant = args.data[0].GetVectorType() == VectorType::CONSTANT_VECTOR;
	bool arg1_constant = args.data[1].GetVectorType() == VectorType::CONSTANT_VECTOR;
	if (!(arg0_constant ^ arg1_constant)) {
		// We can't optimize by caching one of the two strings
		BinaryExecutor::Execute<string_t, string_t, double>(args.data[0], args.data[1], result, args.size(), fun);
		return;
	}

	if (arg0_constant) {
		CachedFunction<CACHED_SIMILARITY>(args.data[0], args.data[1], result, args.size());
	} else {
		CachedFunction<CACHED_SIMILARITY>(args.data[1], args.data[0], result, args.size());
	}
}

static void JaroFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	TemplatedJaroWinklerFunction<duckdb_jaro_winkler::CachedJaroSimilarity<char>>(args, result, JaroScalarFunction);
}

static void JaroWinklerFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	TemplatedJaroWinklerFunction<duckdb_jaro_winkler::CachedJaroWinklerSimilarity<char>>(args, result,
	                                                                                     JaroWinklerScalarFunction);
}

void JaroWinklerFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(ScalarFunction("jaro_similarity", {LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::DOUBLE,
	                               JaroFunction));
	set.AddFunction(ScalarFunction("jaro_winkler_similarity", {LogicalType::VARCHAR, LogicalType::VARCHAR},
	                               LogicalType::DOUBLE, JaroWinklerFunction));
}

} // namespace duckdb
