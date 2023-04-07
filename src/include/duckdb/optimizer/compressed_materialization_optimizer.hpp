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

struct CMChildInfo {
public:
	CMChildInfo(LogicalOperator &op, const vector<ColumnBinding> &referenced_bindings);

public:
	const vector<ColumnBinding> bindings;
	const vector<LogicalType> &types;
	vector<bool> is_compress_candidate;
};

struct CompressedMaterializationInfo {
public:
	CompressedMaterializationInfo(LogicalOperator &op, vector<idx_t> &&child_idxs, bool changes_bindings,
	                              const vector<ColumnBinding> &referenced_bindings);

public:
	const vector<ColumnBinding> bindings;
	const vector<LogicalType> &types;
	const bool changes_bindings;

	const vector<idx_t> child_idxs;
	vector<CMChildInfo> child_info;
};

typedef column_binding_map_t<unique_ptr<BaseStatistics>> statistics_map_t;

class CompressedMaterialization {
public:
	explicit CompressedMaterialization(ClientContext &context, statistics_map_t &&statistics_map);

	unique_ptr<LogicalOperator> Optimize(unique_ptr<LogicalOperator> op);

private:
	void Compress(unique_ptr<LogicalOperator> &op);

	void CompressAggregate(unique_ptr<LogicalOperator> *op_ptr);
	void CompressOrder(unique_ptr<LogicalOperator> *op_ptr);

	void CreateProjections(unique_ptr<LogicalOperator> *op_ptr, const CompressedMaterializationInfo &info);
	void CreateCompressProjection(unique_ptr<LogicalOperator> *child_op, vector<unique_ptr<Expression>> &&compressions,
	                              const CMChildInfo &child_info);

	unique_ptr<Expression> GetCompressExpression(const ColumnBinding &binding, const LogicalType &type,
	                                             const bool &is_compress_candidate);
	unique_ptr<Expression> GetIntegralCompress(const ColumnBinding &binding, const LogicalType &type,
	                                           const BaseStatistics &stats);
	unique_ptr<Expression> GetStringCompress(const ColumnBinding &binding, const LogicalType &type,
	                                         const BaseStatistics &stats);

private:
	ClientContext &context;
	statistics_map_t statistics_map;
	LogicalOperator *root;
	idx_t projection_index;
};

} // namespace duckdb
