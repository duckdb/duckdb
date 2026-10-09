#pragma once

#include "duckdb/function/table_function.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/common/reference_map.hpp"
#include "duckdb/execution/operator/join/join_filter_pushdown.hpp"

namespace duckdb {

class LogicalComparisonJoin;
class BinarySerializer;
class BinaryDeserializer;
struct JoinFilterPushdownInfo;

//! Private state for an in-process copy. Never attached to a persistent serializer.
class LogicalOperatorCopyState {
public:
	struct Scan {
		BoundTableFunction function;
		unique_ptr<FunctionData> bind_data;
		virtual_column_map_t virtual_columns;
	};

	idx_t CopyScan(const LogicalGet &get);
	unique_ptr<Scan> TakeScan(idx_t index);
	void VerifyConsumed() const;
	void Validate(const LogicalOperator &op);
	void SerializeJoinExpressions(BinarySerializer &serializer) const;
	void DeserializeJoinExpressions(BinaryDeserializer &deserializer);
	void CopyAnnotations(const LogicalOperator &source, LogicalOperator &target);

private:
	struct DynamicFilters {
		optional_ptr<const LogicalGet> scan;
		vector<reference<const LogicalComparisonJoin>> producers;
		shared_ptr<DynamicTableFilterSet> copy;
		bool attached = false;
	};

	void ValidateOperator(const LogicalOperator &op);
	void ValidateJoin(const LogicalComparisonJoin &join);
	unique_ptr<JoinFilterPushdownInfo> CopyJoinFilters(const JoinFilterPushdownInfo &source);

	vector<unique_ptr<Scan>> scans;
	reference_map_t<const DynamicTableFilterSet, DynamicFilters> dynamic_filters;
	reference_map_t<const LogicalComparisonJoin, unique_ptr<JoinFilterPushdownInfo>> join_filters;
};

} // namespace duckdb
