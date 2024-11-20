#include "jaro_winkler.hpp"

#include "core_functions/scalar/string_functions.hpp"

namespace duckdb {

static inline double JaroScalarFunction(const string_t &s1, const string_t &s2, const double_t &score_cutoff = 0.0) {
	auto s1_begin = s1.GetData();
	auto s2_begin = s2.GetData();
	return duckdb_jaro_winkler::jaro_similarity(s1_begin, s1_begin + s1.GetSize(), s2_begin, s2_begin + s2.GetSize(),
	                                            score_cutoff);
}

static inline double JaroWinklerScalarFunction(const string_t &s1, const string_t &s2,
                                               const double_t &score_cutoff = 0.0) {
	auto s1_begin = s1.GetData();
	auto s2_begin = s2.GetData();
	return duckdb_jaro_winkler::jaro_winkler_similarity(s1_begin, s1_begin + s1.GetSize(), s2_begin,
	                                                    s2_begin + s2.GetSize(), 0.1, score_cutoff);
}

template <class CACHED_SIMILARITY>
static void CachedFunction(Vector &constant, Vector &other, Vector &result, DataChunk &args) {
	auto val = constant.GetValue(0);
	idx_t count = args.size();
	if (val.IsNull()) {
		auto &result_validity = FlatVector::Validity(result);
		result_validity.SetAllInvalid(count);
		return;
	}

	auto str_val = StringValue::Get(val);
	auto cached = CACHED_SIMILARITY(str_val);

	D_ASSERT(args.ColumnCount() == 2 || args.ColumnCount() == 3);
	if (args.ColumnCount() == 2) {
		UnaryExecutor::Execute<string_t, double>(other, result, count, [&](const string_t &other_str) {
			auto other_str_begin = other_str.GetData();
			return cached.similarity(other_str_begin, other_str_begin + other_str.GetSize());
		});
	} else {
		auto score_cutoff = args.data[2];
		BinaryExecutor::Execute<string_t, double_t, double>(
		    other, score_cutoff, result, count, [&](const string_t &other_str, const double_t score_cutoff) {
			    auto other_str_begin = other_str.GetData();
			    return cached.similarity(other_str_begin, other_str_begin + other_str.GetSize(), score_cutoff);
		    });
	}
}

template <class CACHED_SIMILARITY, class SIMILARITY_FUNCTION>
static void TemplatedJaroWinklerFunction(DataChunk &args, Vector &result, SIMILARITY_FUNCTION fun) {
	bool arg0_constant = args.data[0].GetVectorType() == VectorType::CONSTANT_VECTOR;
	bool arg1_constant = args.data[1].GetVectorType() == VectorType::CONSTANT_VECTOR;
	if (!(arg0_constant ^ arg1_constant)) {
		// We can't optimize by caching one of the two strings
		D_ASSERT(args.ColumnCount() == 2 || args.ColumnCount() == 3);
		if (args.ColumnCount() == 2) {
			BinaryExecutor::Execute<string_t, string_t, double>(
			    args.data[0], args.data[1], result, args.size(),
			    [&](const string_t &s1, const string_t &s2) { return fun(s1, s2, 0.0); });
			return;
		} else {
			TernaryExecutor::Execute<string_t, string_t, double_t, double>(args.data[0], args.data[1], args.data[2],
			                                                               result, args.size(), fun);
			return;
		}
	}

	if (arg0_constant) {
		CachedFunction<CACHED_SIMILARITY>(args.data[0], args.data[1], result, args);
	} else {
		CachedFunction<CACHED_SIMILARITY>(args.data[1], args.data[0], result, args);
	}
}

static void JaroFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	TemplatedJaroWinklerFunction<duckdb_jaro_winkler::CachedJaroSimilarity<char>>(args, result, JaroScalarFunction);
}

static void JaroWinklerFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	TemplatedJaroWinklerFunction<duckdb_jaro_winkler::CachedJaroWinklerSimilarity<char>>(args, result,
	                                                                                     JaroWinklerScalarFunction);
}

ScalarFunctionSet JaroSimilarityFun::GetFunctions() {
	ScalarFunctionSet jaro;

	const auto list_type = LogicalType::LIST(LogicalType::VARCHAR);
	auto fun = ScalarFunction({LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::DOUBLE, JaroFunction);
	jaro.AddFunction(fun);

	fun = ScalarFunction({LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::DOUBLE}, LogicalType::DOUBLE,
	                     JaroFunction);
	jaro.AddFunction(fun);
	return jaro;
}

ScalarFunctionSet JaroWinklerSimilarityFun::GetFunctions() {
	ScalarFunctionSet jaroWinkler;

	const auto list_type = LogicalType::LIST(LogicalType::VARCHAR);
	auto fun = ScalarFunction({LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::DOUBLE, JaroWinklerFunction);
	jaroWinkler.AddFunction(fun);

	fun = ScalarFunction({LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::DOUBLE}, LogicalType::DOUBLE,
	                     JaroWinklerFunction);
	jaroWinkler.AddFunction(fun);
	return jaroWinkler;
}

} // namespace duckdb
