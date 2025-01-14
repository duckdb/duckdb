#include "duckdb/common/map.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "core_functions/scalar/string_functions.hpp"

#include <bitset>
#include <ctype.h>

namespace duckdb {

namespace {
constexpr size_t MAX_SIZE = std::numeric_limits<unsigned char>::max() + 1;
}

static inline std::bitset<MAX_SIZE> GetSet(const string_t &str) {
	std::bitset<MAX_SIZE> array_set;

	idx_t str_len = str.GetSize();
	auto s = str.GetData();

	for (idx_t pos = 0; pos < str_len; pos++) {
		array_set.set(static_cast<unsigned char>(s[pos]));
	}
	return array_set;
}

static double JaccardSimilarity(const string_t &str, const string_t &txt) {
	if (str.GetSize() < 1 || txt.GetSize() < 1) {
		throw InvalidInputException("Jaccard Function: An argument too short!");
	}
	std::bitset<MAX_SIZE> m_str, m_txt;

	m_str = GetSet(str);
	m_txt = GetSet(txt);

	idx_t size_intersect = (m_str & m_txt).count();
	idx_t size_union = (m_str | m_txt).count();

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
