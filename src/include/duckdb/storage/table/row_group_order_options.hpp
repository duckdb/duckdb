//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/table/row_group_order_options.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/enums/order_type.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/storage/storage_index.hpp"

namespace duckdb {
class Serializer;
class Deserializer;

enum class OrderByStatistics : uint8_t { MIN, MAX };
enum class OrderByColumnType : uint8_t { NUMERIC, STRING };

struct RowGroupOrderOptions {
	RowGroupOrderOptions(const StorageIndex &column_idx_p, OrderByStatistics order_by_p, OrderType order_type_p,
	                     OrderByNullType null_order_p, OrderByColumnType column_type_p,
	                     optional_idx row_limit_p = optional_idx(), idx_t row_group_offset_p = 0,
	                     idx_t leading_null_group_offset_p = 0)
	    : column_idx(column_idx_p), order_by(order_by_p), order_type(order_type_p), null_order(null_order_p),
	      column_type(column_type_p), row_limit(row_limit_p), row_group_offset(row_group_offset_p),
	      leading_null_group_offset(leading_null_group_offset_p) {
	}

	const StorageIndex column_idx;
	const OrderByStatistics order_by;
	const OrderType order_type;
	const OrderByNullType null_order;
	const OrderByColumnType column_type;
	const optional_idx row_limit;
	const idx_t row_group_offset;
	const idx_t leading_null_group_offset;

	void Serialize(Serializer &serializer) const;
	static unique_ptr<RowGroupOrderOptions> Deserialize(Deserializer &deserializer);
};

} // namespace duckdb
