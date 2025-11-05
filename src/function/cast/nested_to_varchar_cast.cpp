#include "duckdb/function/cast/nested_to_varchar_cast.hpp"

namespace duckdb {

const bool NestedToVarcharCast::LOOKUP_TABLE[256] = {
    false, false, false, false, false, false, false, false, false, false, false, false, false, false, false,
    false, false, false, false, false, false, false, false, false, false, false, false, false, false, false,
    false, false, false, false,
    true, // "
    false, false, false, false,
    true, // '
    true, // (
    true, // )
    false, false,
    true, // ,
    false, false, false, false, false, false, false, false, false, false, false, false, false,
    true, // :
    false, false,
    true, // =
    false, false, false, false, false, false, false, false, false, false, false, false, false, false, false,
    false, false, false, false, false, false, false, false, false, false, false, false, false, false,
    true, // [
    false,
    true, // ]
    false, false, false, false, false, false, false, false, false, false, false, false, false, false, false,
    false, false, false, false, false, false, false, false, false, false, false, false, false, false,
    true, // {
    false,
    true, // }
    false, false, false, false, false, false, false, false, false, false, false, false, false, false, false,
    false, false, false, false, false, false, false, false, false, false, false, false, false, false, false,
    false, false, false, false, false, false, false, false, false, false, false, false, false, false, false,
    false, false, false, false, false, false, false, false, false, false, false, false, false, false, false,
    false, false, false, false, false, false, false, false, false, false, false, false, false, false, false,
    false, false, false, false, false, false, false, false, false, false, false, false, false, false, false,
    false, false, false, false, false, false, false, false, false, false, false, false, false, false, false,
    false, false, false, false, false, false, false, false, false, false, false, false, false, false, false,
    false, false, false, false, false, false, false, false, false, false};

} // namespace duckdb
