
//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/filter/dynamic_filter.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/table_filter.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/enums/expression_type.hpp"
#include "duckdb/common/atomic.hpp"
#include "duckdb/common/mutex.hpp"

namespace duckdb {

struct DynamicFilterData {
	DynamicFilterData() : comparison_type(ExpressionType::INVALID) {
	}
	DynamicFilterData(ExpressionType comparison_type_p, Value constant_p);

	mutex lock;
	ExpressionType comparison_type;
	Value constant;
	atomic<bool> initialized = {false};

	void SetValue(Value val);
	void Reset();
	static bool CompareValue(ExpressionType comparison_type, const Value &constant, const Value &value);
	static FilterPropagateResult CheckStatistics(BaseStatistics &stats, ExpressionType comparison_type,
	                                             const Value &constant);
};

//! DEPRECATED - only preserved for backwards-compatible deserialization and expression conversion
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
