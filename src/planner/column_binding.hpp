//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/column_binding.hpp
//
//
//===----------------------------------------------------------------------===//
#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/optimizer/cascade/utils.h"
#include <functional>

using namespace gpos;

namespace duckdb
{

struct ColumnBinding
{
	idx_t table_index;
	// This index is local to a Binding, and has no meaning outside of the context of the Binding that created it
	idx_t column_index;

	ColumnBinding() : table_index(DConstants::INVALID_INDEX), column_index(DConstants::INVALID_INDEX)
	{
	}

	ColumnBinding(idx_t table, idx_t column) : table_index(table), column_index(column)
	{
	}

	bool operator==(const ColumnBinding &rhs) const
	{
		return table_index == rhs.table_index && column_index == rhs.column_index;
	}

	bool operator<( const ColumnBinding &rhs ) const
    {
       return (table_index < rhs.table_index) && (column_index < rhs.column_index);
    }

	ULONG HashValue()
	{
		return gpos::CombineHashes(gpos::HashValue(&table_index), gpos::HashValue(&column_index));
	}
};
} // namespace duckdb