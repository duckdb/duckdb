#include "duckdb/common/map.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/core_functions/scalar/string_functions.hpp"

#include <array>
#include <ctype.h>

namespace duckdb {

namespace {
constexpr size_t MAX_SIZE = std::numeric_limits<unsigned char>::max() + 1;
}

static inline std::array<char, MAX_SIZE> GetSet(const string_t &str) {
	std::array<char, MAX_SIZE> array_set;
	array_set.fill(0);

	idx_t str_len = str.GetSize();
	auto s = str.GetData();

	for (idx_t pos = 0; pos < str_len; pos++) {
		array_set[static_cast<unsigned char>(s[pos])] = 1;
	}
	return array_set;
}

static double JaccardSimilarity(const string_t &str, const string_t &txt) {
	if (str.GetSize() < 1 || txt.GetSize() < 1) {
		throw InvalidInputException("Jaccard Function: An argument too short!");
	}
	std::array<char, MAX_SIZE> m_str, m_txt;

	m_str = GetSet(str);
	m_txt = GetSet(txt);

	idx_t size_intersect = 0;
	idx_t size_union = 0;
	for (size_t i = 0; i < MAX_SIZE; ++i) {
		size_intersect += m_str[i] & m_txt[i];
		size_union += m_str[i] | m_txt[i];
	}

	return static_cast<double>(size_intersect) / static_cast<double>(size_union);
}

static double JaccardScalarFunction(Vector &result, const string_t str, string_t tgt) {
	return (double)JaccardSimilarity(str, tgt);
}

static void JaccardFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &str_vec = args.data[0];
	auto &tgt_vec = args.data[1];

	BinaryExecutor::Execute<string_t, string_t, double>(
	    str_vec, tgt_vec, result, args.size(),
	    [&](string_t str, string_t tgt) { return JaccardScalarFunction(result, str, tgt); });
}

ScalarFunction JaccardFun::GetFunction() {
	return ScalarFunction({LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::DOUBLE, JaccardFunction);
}

} // namespace duckdb
