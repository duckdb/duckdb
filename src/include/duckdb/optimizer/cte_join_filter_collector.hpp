//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/cte_join_filter_collector.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/enums/expression_type.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/planner/column_binding.hpp"

namespace duckdb {

class LogicalOperator;
class LogicalCTERef;

struct CTEJoinFilter {
	TableIndex source;
	TableIndex target;
	TableIndex target_scan;
	vector<ProjectionIndex> source_columns;
	vector<ProjectionIndex> target_columns;
	vector<ExpressionType> comparisons;
};

class CTEJoinFilterCollector {
public:
	static vector<CTEJoinFilter> Collect(LogicalOperator &op, const unordered_map<TableIndex, TableIndex> &targets);

private:
	explicit CTEJoinFilterCollector(const unordered_map<TableIndex, TableIndex> &targets);
	void VisitOperator(LogicalOperator &op);
	void PushFilter(LogicalOperator &op, const LogicalCTERef &source, const vector<ColumnBinding> &source_keys,
	                vector<ColumnBinding> target_keys, const vector<ExpressionType> &comparisons);
	void AddFilter(const LogicalCTERef &source, const LogicalCTERef &target, const vector<ColumnBinding> &source_keys,
	               const vector<ColumnBinding> &target_keys, const vector<ExpressionType> &comparisons);

private:
	const unordered_map<TableIndex, TableIndex> &join_targets;
	vector<CTEJoinFilter> join_filters;
};

} // namespace duckdb
