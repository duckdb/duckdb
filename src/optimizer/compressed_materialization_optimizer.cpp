#include "duckdb/optimizer/compressed_materialization_optimizer.hpp"

#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/scalar/operators.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

namespace duckdb {

CompressedMaterializationChildInfo::CompressedMaterializationChildInfo(LogicalOperator &op,
                                                                       const vector<ColumnBinding> &referenced_bindings)
    : bindings(op.GetColumnBindings()), types(op.types), is_compress_candidate(bindings.size(), true) {
	for (const auto &binding : referenced_bindings) {
		for (idx_t binding_idx = 0; binding_idx < bindings.size(); binding_idx++) {
			if (binding == bindings[binding_idx]) {
				is_compress_candidate[binding_idx] = false;
			}
		}
	}
}

CompressedMaterializationInfo::CompressedMaterializationInfo(LogicalOperator &op, const vector<idx_t> &child_idxs,
                                                             const bool changes_bdingins_p,
                                                             const vector<ColumnBinding> &referenced_bindings)
    : bindings(op.GetColumnBindings()), types(op.types), changes_bindings(changes_bdingins_p), child_idxs(child_idxs) {
	child_info.reserve(child_idxs.size());
	for (const auto &child_idx : child_idxs) {
		child_info.emplace_back(*op.children[child_idx], referenced_bindings);
	}
}

CompressedMaterializationOptimizer::CompressedMaterializationOptimizer(
    ClientContext &context_p, column_binding_map_t<unique_ptr<BaseStatistics>> &&statistics_map_p)
    : context(context_p), statistics_map(std::move(statistics_map_p)) {
}

idx_t FindMaxTableIndex(LogicalOperator &op) {
	idx_t max = 0;
	for (const auto &child : op.children) {
		max = MaxValue<idx_t>(max, FindMaxTableIndex(*child));
	}
	for (const auto &binding : op.GetColumnBindings()) {
		max = MaxValue<idx_t>(max, binding.table_index);
	}
	return max;
}

unique_ptr<LogicalOperator> CompressedMaterializationOptimizer::Optimize(unique_ptr<LogicalOperator> op) {
	root = op.get();
	root->ResolveOperatorTypes();
	table_index = FindMaxTableIndex(*root);
	Compress(op);
	// TODO remove redundant (de)compressions
	return op;
}

void CompressedMaterializationOptimizer::Compress(unique_ptr<LogicalOperator> &op) {
	for (auto &child : op->children) {
		Compress(child);
	}

	switch (op->type) {
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY:
		CompressAggregate(&op);
		break;
	case LogicalOperatorType::LOGICAL_ORDER_BY:
		CompressOrder(&op);
		break;
	default:
		break;
	}
}

void CompressedMaterializationOptimizer::CreateProjections(unique_ptr<LogicalOperator> *op_ptr,
                                                           const CompressedMaterializationInfo &info) {
	// TODO potentially wrap op_ptr with
	auto &op = **op_ptr;
	for (idx_t i = 0; i < info.child_idxs.size(); i++) {
		auto &child_op = op.children[info.child_idxs[i]];
		const auto &child_info = info.child_info[i];

		vector<unique_ptr<Expression>> compress_expressions;
		for (idx_t child_i = 0; child_i < child_info.bindings.size(); child_i++) {
			const auto &child_binding = child_info.bindings[child_i];
			auto it = statistics_map.find(child_binding);
			if (child_info.is_compress_candidate[child_i] && it != statistics_map.end() && it->second) {

			} else {
				// TODO just a boundcolrefexpr
			}
		}
	}
}

bool CompressedMaterializationOptimizer::GetExpressions(const ColumnBinding &binding, const LogicalType &type,
                                                        const BaseStatistics &stats,
                                                        vector<unique_ptr<Expression>> &compress_expressions,
                                                        vector<unique_ptr<Expression>> &decompress_expressions) {
	if (TypeIsIntegral(type.InternalType()) &&
	    GetIntegralExpressions(binding, type, stats, compress_expressions, decompress_expressions)) {
		return true;
	} else if (type.id() == LogicalTypeId::VARCHAR) {

	} else {

		return false;
	}
}

bool CompressedMaterializationOptimizer::GetIntegralExpressions(
    const ColumnBinding &binding, const LogicalType &type, const BaseStatistics &stats,
    vector<unique_ptr<Expression>> &compress_expressions, vector<unique_ptr<Expression>> &decompress_expressions) {
	if (GetTypeIdSize(type.InternalType()) == 1 || !NumericStats::HasMinMax(stats)) {
		return false;
	}

	auto min = make_uniq<BoundConstantExpression>(NumericStats::Min(stats));

	// Create expression for max - min to get the range of values
	vector<unique_ptr<Expression>> arguments;
	arguments.emplace_back(make_uniq<BoundConstantExpression>(NumericStats::Max(stats)));
	arguments.emplace_back(min->Copy());
	BoundFunctionExpression sub(type, SubtractFun::GetFunction(type), std::move(arguments), nullptr);

	// Evaluate and cast to UBIGINT (might fail for HUGEINT, in which case we just return false)
	Value range_value = ExpressionExecutor::EvaluateScalar(context, sub);
	if (!range_value.DefaultTryCastAs(LogicalType::UBIGINT)) {
		return false;
	}

	// Get the smallest type that the range can fit into
	LogicalType cast_type;
	const auto range = UBigIntValue::Get(range_value);
	if (range < NumericLimits<uint8_t>().Maximum()) {
		cast_type = LogicalType::UTINYINT;
	} else if (range < NumericLimits<uint16_t>().Maximum()) {
		cast_type = LogicalType::USMALLINT;
	} else if (range < NumericLimits<uint32_t>().Maximum()) {
		cast_type = LogicalType::UINTEGER;
	} else {
		D_ASSERT(range < NumericLimits<uint64_t>().Maximum());
		cast_type = LogicalType::UBIGINT;
	}

	if (GetTypeIdSize(cast_type.InternalType()) == GetTypeIdSize(type.InternalType())) {
		return false;
	}
	D_ASSERT(GetTypeIdSize(cast_type.InternalType()) < GetTypeIdSize(type.InternalType()));

	// Type that the range fits into is smaller than the input type
}

} // namespace duckdb
