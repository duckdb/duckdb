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
	//! Bindings and types before compressing
	vector<ColumnBinding> bindings_before;
	vector<LogicalType> &types;
	//! Whether the input binding is eligible for compression
	vector<bool> can_compress;

	//! Bindings after compressing (projection on top)
	vector<ColumnBinding> bindings_after;
};

struct CompressedMaterializationInfo {
public:
	CompressedMaterializationInfo(LogicalOperator &op, vector<idx_t> &&child_idxs,
	                              const vector<ColumnBinding> &referenced_bindings);

public:
	//! Bindings and types before compressing
	vector<ColumnBinding> bindings;
	vector<LogicalType> &types;

	//! Operator child info
	vector<idx_t> child_idxs;
	vector<CMChildInfo> child_info;
};

typedef column_binding_map_t<unique_ptr<BaseStatistics>> statistics_map_t;

//! The CompressedMaterialization optimizer compressed columns using projections, based on available statistics,
//! but only if the data enters a materializing operator
class CompressedMaterialization {
public:
	explicit CompressedMaterialization(ClientContext &context, statistics_map_t &&statistics_map);

	unique_ptr<LogicalOperator> Optimize(unique_ptr<LogicalOperator> op);

private:
	//! Depth-first traversal of the plan
	void Compress(unique_ptr<LogicalOperator> &op);

	//! Compress call for materializing operators
	void CompressAggregate(unique_ptr<LogicalOperator> *op_ptr);
	void CompressOrder(unique_ptr<LogicalOperator> *op_ptr);

	//! Create projections around materializing operators
	void CreateProjections(unique_ptr<LogicalOperator> *op_ptr, CompressedMaterializationInfo &info);
	bool TryCompressChild(CompressedMaterializationInfo &info, const CMChildInfo &child_info,
	                      vector<unique_ptr<Expression>> &compress_expressions);
	void CreateCompressProjection(unique_ptr<LogicalOperator> *child_op,
	                              vector<unique_ptr<Expression>> &&compress_exprs, CMChildInfo &child_info);
	void CreateDecompressProjection(unique_ptr<LogicalOperator> *parent_op,
	                                vector<unique_ptr<Expression>> &&decompress_exprs,
	                                unique_ptr<LogicalOperator> *child_op, const CMChildInfo &child_info);

	//! Create expressions that apply a scalar compression function
	unique_ptr<Expression> GetCompressExpression(const ColumnBinding &binding, const LogicalType &type,
	                                             const bool &can_compress);
	unique_ptr<Expression> GetCompressExpression(unique_ptr<Expression> input, const BaseStatistics &stats);
	unique_ptr<Expression> GetIntegralCompress(unique_ptr<Expression> input, const BaseStatistics &stats);
	unique_ptr<Expression> GetStringCompress(unique_ptr<Expression> input, const BaseStatistics &stats);

private:
	ClientContext &context;
	statistics_map_t statistics_map;
	LogicalOperator *root;
	idx_t projection_index;
};

} // namespace duckdb
