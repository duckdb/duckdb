
//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/filter/dynamic_filter.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <atomic>
#include <string>

#include "duckdb/planner/table_filter.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/enums/expression_type.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/common/atomic.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/enums/filter_propagate_result.hpp"
#include "duckdb/common/shared_ptr_ipp.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/planner/expression.hpp"

namespace duckdb {
class BaseStatistics;
class Deserializer;
class Serializer;

struct DynamicFilterData {
	mutex lock;
	unique_ptr<ConstantFilter> filter;
	atomic<bool> initialized = {false};

	void SetValue(Value val);
	void Reset();
};

class DynamicFilter : public TableFilter {
public:
	static constexpr const TableFilterType TYPE = TableFilterType::DYNAMIC_FILTER;

public:
	DynamicFilter();
	explicit DynamicFilter(shared_ptr<DynamicFilterData> filter_data);

	//! The shared, dynamic filter data
	shared_ptr<DynamicFilterData> filter_data;

public:
	FilterPropagateResult CheckStatistics(BaseStatistics &stats) const override;
	string ToString(const string &column_name) const override;
	bool Equals(const TableFilter &other) const override;
	unique_ptr<TableFilter> Copy() const override;
	unique_ptr<Expression> ToExpression(const Expression &column) const override;
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<TableFilter> Deserialize(Deserializer &deserializer);
};

} // namespace duckdb
