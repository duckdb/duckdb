//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/compressed_materialization_optimizer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/scalar/compressed_materialization_functions.hpp"
#include "duckdb/planner/column_binding_map.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"

namespace duckdb {

class LogicalOperator;

struct CompressedMaterializationChildInfo {
public:
	CompressedMaterializationChildInfo(LogicalOperator &op, const vector<ColumnBinding> &referenced_bindings);

public:
	const vector<ColumnBinding> bindings;
	const vector<LogicalType> &types;
	vector<bool> is_compress_candidate;
};

struct CompressedMaterializationInfo {
public:
	CompressedMaterializationInfo(LogicalOperator &op, const vector<idx_t> &child_idxs, const bool changes_bindings,
	                              const vector<ColumnBinding> &referenced_bindings);

public:
	const vector<ColumnBinding> bindings;
	const vector<LogicalType> &types;
	const bool changes_bindings;

	const vector<idx_t> child_idxs;
	vector<CompressedMaterializationChildInfo> child_info;
};

class CompressedMaterializationOptimizer {
public:
	explicit CompressedMaterializationOptimizer(ClientContext &context,
	                                            column_binding_map_t<unique_ptr<BaseStatistics>> &&statistics_map);

	unique_ptr<LogicalOperator> Optimize(unique_ptr<LogicalOperator> op);

private:
	void Compress(unique_ptr<LogicalOperator> &op);

	void CompressAggregate(unique_ptr<LogicalOperator> *op_ptr);
	void CompressOrder(unique_ptr<LogicalOperator> *op_ptr);

	void CreateProjections(unique_ptr<LogicalOperator> *op_ptr, const CompressedMaterializationInfo &info);
	bool GetExpressions(const ColumnBinding &binding, const LogicalType &type, const BaseStatistics &stats,
	                    vector<unique_ptr<Expression>> &compress_expressions,
	                    vector<unique_ptr<Expression>> &decompress_expressions);
	bool GetIntegralExpressions(const ColumnBinding &binding, const LogicalType &type, const BaseStatistics &stats,
	                            vector<unique_ptr<Expression>> &compress_expressions,
	                            vector<unique_ptr<Expression>> &decompress_expressions);
	bool GetStringExpressions(const ColumnBinding &binding, const LogicalType &type, const BaseStatistics &stats,
	                          vector<unique_ptr<Expression>> &compress_expressions,
	                          vector<unique_ptr<Expression>> &decompress_expressions);

private:
	ClientContext &context;
	column_binding_map_t<unique_ptr<BaseStatistics>> statistics_map;
	LogicalOperator *root;
	idx_t table_index;
};

} // namespace duckdb
