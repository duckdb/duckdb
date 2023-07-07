//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/compressed_materialization.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/unordered_set.hpp"
#include "duckdb/function/scalar/compressed_materialization_functions.hpp"
#include "duckdb/planner/column_binding_map.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"

namespace duckdb {

class LogicalOperator;
struct JoinCondition;

struct CMChildInfo {
public:
	CMChildInfo(LogicalOperator &op, const column_binding_set_t &referenced_bindings);

public:
	//! Bindings and types before compressing
	vector<ColumnBinding> bindings_before;
	vector<LogicalType> &types;
	//! Whether the input binding is eligible for compression
	vector<bool> can_compress;

	//! Bindings after compressing (projection on top)
	vector<ColumnBinding> bindings_after;
};

struct CMBindingInfo {
public:
	explicit CMBindingInfo(ColumnBinding binding, const LogicalType &type);

public:
	ColumnBinding binding;

	//! Type before compressing
	LogicalType type;
	bool needs_decompression;
	unique_ptr<BaseStatistics> stats;
};

struct CompressedMaterializationInfo {
public:
	CompressedMaterializationInfo(LogicalOperator &op, vector<idx_t> &&child_idxs,
	                              const column_binding_set_t &referenced_bindings);

public:
	//! Mapping from incoming bindings to outgoing bindings
	column_binding_map_t<CMBindingInfo> binding_map;

	//! Operator child info
	vector<idx_t> child_idxs;
	vector<CMChildInfo> child_info;
};

struct CompressExpression {
public:
	CompressExpression(unique_ptr<Expression> expression, unique_ptr<BaseStatistics> stats);

public:
	unique_ptr<Expression> expression;
	unique_ptr<BaseStatistics> stats;
};

typedef column_binding_map_t<unique_ptr<BaseStatistics>> statistics_map_t;

//! The CompressedMaterialization optimizer compressed columns using projections, based on available statistics,
//! but only if the data enters a materializing operator
class CompressedMaterialization {
public:
	explicit CompressedMaterialization(ClientContext &context, Binder &binder, statistics_map_t &&statistics_map);

	void Compress(unique_ptr<LogicalOperator> &op);

private:
	//! Depth-first traversal of the plan
	void CompressInternal(unique_ptr<LogicalOperator> &op);

	//! Compress materializing operators
	void CompressAggregate(unique_ptr<LogicalOperator> &op);
	void CompressDistinct(unique_ptr<LogicalOperator> &op);
	void CompressOrder(unique_ptr<LogicalOperator> &op);

	//! Update statistics after compressing
	void UpdateAggregateStats(unique_ptr<LogicalOperator> &op);
	void UpdateOrderStats(unique_ptr<LogicalOperator> &op);

	//! Adds bindings referenced in expression to referenced_bindings
	static void GetReferencedBindings(const Expression &expression, column_binding_set_t &referenced_bindings);
	//! Updates CMBindingInfo in the binding_map in info
	void UpdateBindingInfo(CompressedMaterializationInfo &info, const ColumnBinding &binding, bool needs_decompression);

	//! Create (de)compress projections around the operator
	void CreateProjections(unique_ptr<LogicalOperator> &op, CompressedMaterializationInfo &info);
	bool TryCompressChild(CompressedMaterializationInfo &info, const CMChildInfo &child_info,
	                      vector<unique_ptr<CompressExpression>> &compress_expressions);
	void CreateCompressProjection(unique_ptr<LogicalOperator> &child_op,
	                              vector<unique_ptr<CompressExpression>> &&compress_exprs,
	                              CompressedMaterializationInfo &info, CMChildInfo &child_info);
	void CreateDecompressProjection(unique_ptr<LogicalOperator> &op, CompressedMaterializationInfo &info);

	//! Create expressions that apply a scalar compression function
	unique_ptr<CompressExpression> GetCompressExpression(const ColumnBinding &binding, const LogicalType &type,
	                                                     const bool &can_compress);
	unique_ptr<CompressExpression> GetCompressExpression(unique_ptr<Expression> input, const BaseStatistics &stats);
	unique_ptr<CompressExpression> GetIntegralCompress(unique_ptr<Expression> input, const BaseStatistics &stats);
	unique_ptr<CompressExpression> GetStringCompress(unique_ptr<Expression> input, const BaseStatistics &stats);

	//! Create an expression that applies a scalar decompression function
	unique_ptr<Expression> GetDecompressExpression(unique_ptr<Expression> input, const LogicalType &result_type,
	                                               const BaseStatistics &stats);
	unique_ptr<Expression> GetIntegralDecompress(unique_ptr<Expression> input, const LogicalType &result_type,
	                                             const BaseStatistics &stats);
	unique_ptr<Expression> GetStringDecompress(unique_ptr<Expression> input, const BaseStatistics &stats);

private:
	ClientContext &context;
	Binder &binder;
	statistics_map_t statistics_map;
	unordered_set<idx_t> compression_table_indices;
	unordered_set<idx_t> decompression_table_indices;
	optional_ptr<LogicalOperator> root;
};

} // namespace duckdb
