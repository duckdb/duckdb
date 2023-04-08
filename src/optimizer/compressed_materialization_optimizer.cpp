#include "duckdb/optimizer/compressed_materialization_optimizer.hpp"

#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/scalar/operators.hpp"
#include "duckdb/optimizer/column_binding_replacer.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"

namespace duckdb {

CMChildInfo::CMChildInfo(LogicalOperator &op, const vector<ColumnBinding> &referenced_bindings)
    : bindings(op.GetColumnBindings()), types(op.types), is_compress_candidate(bindings.size(), true) {
	for (const auto &binding : referenced_bindings) {
		for (idx_t binding_idx = 0; binding_idx < bindings.size(); binding_idx++) {
			if (binding == bindings[binding_idx]) {
				is_compress_candidate[binding_idx] = false;
			}
		}
	}
}

CompressedMaterializationInfo::CompressedMaterializationInfo(LogicalOperator &op, vector<idx_t> &&child_idxs_p,
                                                             bool changes_bindings_p,
                                                             const vector<ColumnBinding> &referenced_bindings)
    : bindings(op.GetColumnBindings()), types(op.types), needs_decompression(bindings.size(), false),
      changes_bindings(changes_bindings_p), child_idxs(child_idxs_p) {
	child_info.reserve(child_idxs.size());
	for (const auto &child_idx : child_idxs) {
		child_info.emplace_back(*op.children[child_idx], referenced_bindings);
	}
}

CompressedMaterialization::CompressedMaterialization(ClientContext &context_p, statistics_map_t &&statistics_map_p)
    : context(context_p), statistics_map(std::move(statistics_map_p)) {
}

static idx_t FindMaxTableIndex(LogicalOperator &op) {
	idx_t max = 0;
	for (const auto &child : op.children) {
		max = MaxValue<idx_t>(max, FindMaxTableIndex(*child));
	}
	for (const auto &binding : op.GetColumnBindings()) {
		max = MaxValue<idx_t>(max, binding.table_index);
	}
	return max;
}

unique_ptr<LogicalOperator> CompressedMaterialization::Optimize(unique_ptr<LogicalOperator> op) {
	root = op.get();
	root->ResolveOperatorTypes();
	projection_index = FindMaxTableIndex(*root) + 1;
	Compress(op);
	// TODO remove redundant (de)compressions
	return op;
}

void CompressedMaterialization::Compress(unique_ptr<LogicalOperator> &op) {
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

void SetNeedsDecompression(CompressedMaterializationInfo &info, const ColumnBinding &needle) {
	const auto &haystack = info.bindings;
	for (idx_t i = 0; i < haystack.size(); i++) {
		const auto &binding = haystack[i];
		if (binding == needle) {
			info.needs_decompression[i] = true;
			break;
		}
	}
}

void CompressedMaterialization::CreateProjections(unique_ptr<LogicalOperator> *op_ptr,
                                                  CompressedMaterializationInfo &info) {
	// TODO: if info.changes_bindings we need to do something more clever for the decompress projection
	auto &materializing_op = **op_ptr;
	vector<bool> needs_decompression(info.bindings.size(), false);
	for (idx_t i = 0; i < info.child_idxs.size(); i++) {
		const auto &child_info = info.child_info[i];
		vector<unique_ptr<Expression>> compress_expressions;
		if (TryCompressChild(info, child_info, compress_expressions)) {
			// We can compress: Create a projection on top of the child operator
			auto child_op = &materializing_op.children[info.child_idxs[i]];
			CreateCompressProjection(child_op, std::move(compress_expressions), child_info);
		}
	}

	// TODO: decompress
}

bool CompressedMaterialization::TryCompressChild(CompressedMaterializationInfo &info, const CMChildInfo &child_info,
                                                 vector<unique_ptr<Expression>> &compress_expressions) {
	// Try to compress each of the column bindings of the child
	bool compressed_anything = false;
	for (idx_t child_i = 0; child_i < child_info.bindings.size(); child_i++) {
		const auto child_binding = child_info.bindings[child_i];
		const auto &child_type = child_info.types[child_i];
		const auto &is_compress_candidate = child_info.is_compress_candidate[child_i];
		auto compress_expr = GetCompressExpression(child_binding, child_type, is_compress_candidate);
		if (compress_expr) { // We compressed, mark the outgoing binding in need of decompression
			compress_expressions.emplace_back(std::move(compress_expr));
			SetNeedsDecompression(info, child_binding);
			compressed_anything = true;
		} else { // We did not compress, just push a colref
			compress_expressions.emplace_back(make_uniq<BoundColumnRefExpression>(child_type, child_binding));
		}
	}
	return compressed_anything;
}

void CompressedMaterialization::CreateCompressProjection(unique_ptr<LogicalOperator> *child_op,
                                                         vector<unique_ptr<Expression>> &&compressions,
                                                         const CMChildInfo &child_info) {
	auto compress_projection = make_uniq<LogicalProjection>(projection_index++, std::move(compressions));
	compress_projection->children.emplace_back(std::move(*child_op));

	// Get the new bindings and types
	const auto new_bindings = compress_projection->GetColumnBindings();
	compress_projection->ResolveOperatorTypes();
	const auto &new_types = compress_projection->types;

	// Move the projection under the parent operator
	*child_op = std::move(compress_projection);

	// Initialize a ColumnBindingReplacer with the new bindings and types
	ColumnBindingReplacer replacer;
	auto &replace_bindings = replacer.replace_bindings;
	for (idx_t col_idx = 0; col_idx < child_info.bindings.size(); col_idx++) {
		const auto &old_binding = child_info.bindings[col_idx];
		const auto &new_binding = new_bindings[col_idx];
		const auto &new_type = new_types[col_idx];
		replace_bindings.emplace_back(old_binding, new_binding, new_type);

		// Remove the old binding from the statistics map
		// FIXME when we decompress we need to re-add the statistics!!
		statistics_map.erase(old_binding);
	}

	// Make the plan consistent again
	replacer.VisitOperator(*root);
}

void CompressedMaterialization::CreateDecompressProjection(unique_ptr<LogicalOperator> *parent_op,
                                                           vector<unique_ptr<Expression>> &&decompressions,
                                                           unique_ptr<LogicalOperator> *child_op,
                                                           const CMChildInfo &child_info) {
}

unique_ptr<Expression> CompressedMaterialization::GetCompressExpression(const ColumnBinding &binding,
                                                                        const LogicalType &type,
                                                                        const bool &is_compress_candidate) {
	auto it = statistics_map.find(binding);
	if (!is_compress_candidate || it == statistics_map.end() || !it->second) {
		auto input = make_uniq<BoundColumnRefExpression>(type, binding);
		const auto &stats = *it->second;
		return GetCompressExpression(std::move(input), stats);
	}
	return nullptr;
}

unique_ptr<Expression> CompressedMaterialization::GetCompressExpression(unique_ptr<duckdb::Expression> input,
                                                                        const duckdb::BaseStatistics &stats) {
	const auto &type = input->return_type;
	if (TypeIsIntegral(type.InternalType())) {
		return GetIntegralCompress(std::move(input), stats);
	} else if (type.id() == LogicalTypeId::VARCHAR) {
		return GetStringCompress(std::move(input), stats);
	}
	return nullptr;
}

static Value GetIntegralRangeValue(ClientContext &context, const LogicalType &type, const BaseStatistics &stats) {
	vector<unique_ptr<Expression>> arguments;
	arguments.emplace_back(make_uniq<BoundConstantExpression>(NumericStats::Max(stats)));
	arguments.emplace_back(make_uniq<BoundConstantExpression>(NumericStats::Min(stats)));
	BoundFunctionExpression sub(type, SubtractFun::GetFunction(type), std::move(arguments), nullptr);
	return ExpressionExecutor::EvaluateScalar(context, sub);
}

unique_ptr<Expression> CompressedMaterialization::GetIntegralCompress(unique_ptr<duckdb::Expression> input,
                                                                      const BaseStatistics &stats) {
	const auto &type = input->return_type;
	if (GetTypeIdSize(type.InternalType()) == 1 || !NumericStats::HasMinMax(stats)) {
		return nullptr;
	}

	// Get range and cast to UBIGINT (might fail for HUGEINT, in which case we just return)
	Value range_value = GetIntegralRangeValue(context, type, stats);
	if (!range_value.DefaultTryCastAs(LogicalType::UBIGINT)) {
		return nullptr;
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
		return nullptr;
	}
	D_ASSERT(GetTypeIdSize(cast_type.InternalType()) < GetTypeIdSize(type.InternalType()));

	// Type that the range fits into is smaller than the input type
	auto compress_function = CMIntegralCompressFun::GetFunction(type, cast_type);
	vector<unique_ptr<Expression>> arguments;
	arguments.emplace_back(std::move(input));
	arguments.emplace_back(make_uniq<BoundConstantExpression>(NumericStats::Min(stats)));
	return make_uniq<BoundFunctionExpression>(cast_type, compress_function, std::move(arguments), nullptr);
}

unique_ptr<Expression> CompressedMaterialization::GetStringCompress(unique_ptr<duckdb::Expression> input,
                                                                    const BaseStatistics &stats) {
	if (!StringStats::HasMaxStringLength(stats)) {
		return nullptr;
	}

	const auto max_string_length = StringStats::MaxStringLength(stats);
	LogicalType cast_type;
	if (max_string_length < 2) {
		cast_type = LogicalType::USMALLINT;
	} else if (max_string_length < 4) {
		cast_type = LogicalType::UINTEGER;
	} else if (max_string_length < 8) {
		cast_type = LogicalType::UBIGINT;
	} else if (max_string_length < 16) {
		cast_type = LogicalType::HUGEINT;
	} else {
		return nullptr;
	}

	auto compress_function = CMStringCompressFun::GetFunction(cast_type);
	vector<unique_ptr<Expression>> arguments;
	arguments.emplace_back(std::move(input));
	return make_uniq<BoundFunctionExpression>(cast_type, compress_function, std::move(arguments), nullptr);
}

} // namespace duckdb
