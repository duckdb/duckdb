//===----------------------------------------------------------------------===//
//                         DuckDB
//
// common/enums/set_operation_type.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

namespace duckdb {

enum class SetOperationType : uint8_t {
	NONE = 0,
	UNION = 1,
	EXCEPT = 2,
	INTERSECT = 3
};

}
