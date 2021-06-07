//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/table_filter.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/enums/filter_propagate_result.hpp"

namespace duckdb {
class BaseStatistics;

enum class TableFilterType : uint8_t {
	CONSTANT_COMPARISON = 0, // constant comparison (e.g. =C, >C, >=C, <C, <=C)
	IS_NULL = 1,
	IS_NOT_NULL = 2,
	CONJUNCTION_OR = 3,
	CONJUNCTION_AND = 4
};

//! TableFilter represents a filter pushed down into the table scan.
class TableFilter {
public:
	TableFilter(TableFilterType filter_type_p) : filter_type(filter_type_p) {
	}
	virtual ~TableFilter() {
	}

	TableFilterType filter_type;

public:
	//! Returns true if the statistics indicate that the segment can contain values that satisfy that filter
	virtual FilterPropagateResult CheckStatistics(BaseStatistics &stats) = 0;
	virtual string ToString(const string &column_name) = 0;
};

class TableFilterSet {
public:
	unordered_map<idx_t, unique_ptr<TableFilter>> filters;

public:
	void PushFilter(idx_t table_index, unique_ptr<TableFilter> filter);
};

} // namespace duckdb
