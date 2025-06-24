#include "duckdb/storage/compression/alp/alp_constants.hpp"

namespace duckdb {

constexpr int64_t AlpConstants::FACT_ARR[];

constexpr float AlpTypedConstants<float>::EXP_ARR[];
constexpr float AlpTypedConstants<float>::FRAC_ARR[];

constexpr double AlpTypedConstants<double>::EXP_ARR[];
constexpr double AlpTypedConstants<double>::FRAC_ARR[];

} // namespace duckdb
