//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/cte_materialize.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"

namespace duckdb {

enum class CTEMaterialize : uint8_t {
	CTE_MATERIALIZE_DEFAULT = 1, /* no option specified */
	CTE_MATERIALIZE_ALWAYS = 2,  /* MATERIALIZED */
	CTE_MATERIALIZE_NEVER = 3    /* NOT MATERIALIZED */
};

} // namespace duckdb
