//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/filter/constant_filter.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/table_filter.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/enums/expression_type.hpp"

namespace duckdb {

// StructFilters can currently only be used in combination with a ConstantFilter
// e.g. SELECT * FROM t WHERE a.b > 2;
// ConstantFilters generally push a IS_NOT_NULL filter on the column they filter, but there is currently no way to
// push a IS_NOT_NULL filter that references another expression (e.g as if was applied  on top of a StructFilter).
// Therefore StructFilters implicitly work as if a IS_NOT_NULL filter has been applied on the extracted column.

class StructFilter : public TableFilter {
public:
	static constexpr const TableFilterType TYPE = TableFilterType::STRUCT_EXTRACT;

public:
	StructFilter(idx_t child_idx, const string &child_name, unique_ptr<TableFilter> child_filter);

	//! The field index to filter on
	idx_t child_idx;

	//! The field name to filter on
	string child_name;

	//! The child filter
	unique_ptr<TableFilter> child_filter;

public:
	FilterPropagateResult CheckStatistics(BaseStatistics &stats) override;
	string ToString(const string &column_name) override;
	bool Equals(const TableFilter &other) const override;
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<TableFilter> Deserialize(Deserializer &deserializer);
};

// SELECT * FROM t WHERE a.b > 2;
// STRUCT ('b', COMP(>, 2));

} // namespace duckdb

// test/sql/storage/types/struct/wal_struct_storage.test
// test/sql/storage/types/struct/struct_storage.test
// test/sql/copy/parquet/writer/write_complex_nested.test:21
// test/sql/copy/parquet/parquet_3896.test:59
// test/sql/copy/parquet/parquet_3896.test
