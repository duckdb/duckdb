//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/filter/perfect_hash_join_filter.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>

#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/planner/table_filter_state.hpp"
#include "duckdb/common/enums/filter_propagate_result.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/planner/expression.hpp"

namespace duckdb {

class PerfectHashJoinExecutor;
class BaseStatistics;
class Deserializer;
class Serializer;
class Vector;
struct JoinFilterTableFilterState;
struct SelectionVector;

class PerfectHashJoinFilter final : public TableFilter {
public:
	static constexpr auto TYPE = TableFilterType::PERFECT_HASH_JOIN_FILTER;

public:
	PerfectHashJoinFilter(optional_ptr<const PerfectHashJoinExecutor> perfect_join_executor,
	                      const string &key_column_name, const LogicalType &key_type_p);

public:
	const LogicalType &GetKeyType() const {
		return key_type;
	}

	FilterPropagateResult CheckStatistics(BaseStatistics &stats) const override;
	string ToString(const string &column_name) const override;

	idx_t Filter(Vector &keys, SelectionVector &sel, idx_t &approved_tuple_count,
	             JoinFilterTableFilterState &state) const;
	bool FilterValue(const Value &value) const;

private:
	bool Equals(const TableFilter &other) const override;
	unique_ptr<TableFilter> Copy() const override;
	unique_ptr<Expression> ToExpression(const Expression &column) const override;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<TableFilter> Deserialize(Deserializer &deserializer);

private:
	optional_ptr<const PerfectHashJoinExecutor> perfect_join_executor;
	const string key_column_name;
	const LogicalType key_type;
};

} // namespace duckdb
