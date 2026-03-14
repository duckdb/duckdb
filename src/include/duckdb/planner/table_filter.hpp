//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/table_filter.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/enums/filter_propagate_result.hpp"
#include "duckdb/planner/column_binding.hpp"
#include "duckdb/common/column_index.hpp"

namespace duckdb {
class BaseStatistics;
class Expression;
class PhysicalOperator;
class PhysicalTableScan;

enum class TableFilterType : uint8_t {
	CONSTANT_COMPARISON = 0,       // constant comparison (e.g. =C, >C, >=C, <C, <=C)
	IS_NULL = 1,                   // C IS NULL
	IS_NOT_NULL = 2,               // C IS NOT NULL
	CONJUNCTION_OR = 3,            // OR of different filters
	CONJUNCTION_AND = 4,           // AND of different filters
	STRUCT_EXTRACT = 5,            // filter applies to child-column of struct
	OPTIONAL_FILTER = 6,           // executing filter is not required for query correctness
	IN_FILTER = 7,                 // col IN (C1, C2, C3, ...)
	DYNAMIC_FILTER = 8,            // dynamic filters can be updated at run-time
	EXPRESSION_FILTER = 9,         // an arbitrary expression
	BLOOM_FILTER = 10,             // a probabilistic filter that can test whether a value is in a set of other value
	PERFECT_HASH_JOIN_FILTER = 11, // perfect hash join probe pushed down
	MAP_EXTRACT = 12,              // filter applies to MAP element access (e.g., map['key'] > value)
};

//! TableFilter represents a filter pushed down into the table scan.
class TableFilter {
public:
	explicit TableFilter(TableFilterType filter_type_p) : filter_type(filter_type_p) {
	}
	virtual ~TableFilter() {
	}

	TableFilterType filter_type;

public:
	//! Returns true if the statistics indicate that the segment can contain values that satisfy that filter
	virtual FilterPropagateResult CheckStatistics(BaseStatistics &stats) const = 0;
	virtual string ToString(const string &column_name) const = 0;
	string DebugToString() const;
	virtual unique_ptr<TableFilter> Copy() const = 0;
	virtual bool Equals(const TableFilter &other) const {
		return filter_type == other.filter_type;
	}
	virtual unique_ptr<Expression> ToExpression(const Expression &column) const = 0;

	virtual void Serialize(Serializer &serializer) const;
	static unique_ptr<TableFilter> Deserialize(Deserializer &deserializer);

public:
	template <class TARGET>
	TARGET &Cast() {
		if (filter_type != TARGET::TYPE) {
			throw InternalException("Failed to cast to type - table filter type mismatch");
		}
		return reinterpret_cast<TARGET &>(*this);
	}

	template <class TARGET>
	const TARGET &Cast() const {
		if (filter_type != TARGET::TYPE) {
			throw InternalException("Failed to cast to type - table filter type mismatch");
		}
		return reinterpret_cast<const TARGET &>(*this);
	}
};

} // namespace duckdb
