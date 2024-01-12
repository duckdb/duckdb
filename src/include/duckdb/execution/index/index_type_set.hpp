//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/index/index_type_set.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/index/index_type.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/optional_ptr.hpp"

namespace duckdb {

class IndexTypeSet {
	mutex lock;
	case_insensitive_map_t<IndexType> functions;

public:
	IndexTypeSet();
	DUCKDB_API optional_ptr<IndexType> FindByName(const string &name);
	DUCKDB_API void RegisterIndexType(const IndexType &index_type);
};

} // namespace duckdb
