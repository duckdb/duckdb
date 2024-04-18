#include "duckdb/optimizer/compressed_materialization.hpp"

#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/scalar/compressed_materialization_functions.hpp"
#include "duckdb/function/scalar/operators.hpp"
#include "duckdb/optimizer/column_binding_replacer.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/optimizer/topn_optimizer.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"

namespace duckdb {

CMChildInfo::CMChildInfo(LogicalOperator &op, const column_binding_set_t &referenced_bindings)
    : bindings_before(op.GetColumnBindings()), types(op.types), can_compress(bindings_before.size(), true) {
	for (const auto &binding : referenced_bindings) {
		for (idx_t binding_idx = 0; binding_idx < bindings_before.size(); binding_idx++) {
			if (binding == bindings_before[binding_idx]) {
				can_compress[binding_idx] = false;
			}
		}
	}
}

CMBindingInfo::CMBindingInfo(ColumnBinding binding_p, const LogicalType &type_p)
    : binding(binding_p), type(type_p), needs_decompression(false) {
}

CompressedMaterializationInfo::CompressedMaterializationInfo(LogicalOperator &op, vector<idx_t> &&child_idxs_p,
                                                             const column_binding_set_t &referenced_bindings)
    : child_idxs(std::move(child_idxs_p)) {
	child_info.reserve(child_idxs.size());
	for (const auto &child_idx : child_idxs) {
		child_info.emplace_back(*op.children[child_idx], referenced_bindings);
	}
}

CompressExpression::CompressExpression(unique_ptr<Expression> expression_p, unique_ptr<BaseStatistics> stats_p)
    : expression(std::move(expression_p)), stats(std::move(stats_p)) {
}

CompressedMaterialization::CompressedMaterialization(Optimizer &optimizer_p, LogicalOperator &root_p,
                                                     statistics_map_t &statistics_map_p)
    : optimizer(optimizer_p), context(optimizer.context), root(&root_p), statistics_map(statistics_map_p) {
}

void CompressedMaterialization::GetReferencedBindings(const Expression &expression,
                                                      column_binding_set_t &referenced_bindings) {
	if (expression.GetExpressionType() == ExpressionType::BOUND_COLUMN_REF) {
		const auto &col_ref = expression.Cast<BoundColumnRefExpression>();
		referenced_bindings.insert(col_ref.binding);
	} else {
		ExpressionIterator::EnumerateChildren(
		    expression, [&](const Expression &child) { GetReferencedBindings(child, referenced_bindings); });
	}
}

void CompressedMaterialization::UpdateBindingInfo(CompressedMaterializationInfo &info, const ColumnBinding &binding,
                                                  bool needs_decompression) {
	auto &binding_map = info.binding_map;
	auto binding_it = binding_map.find(binding);
	if (binding_it == binding_map.end()) {
		return;
	}

	auto &binding_info = binding_it->second;
	binding_info.needs_decompression = needs_decompression;
	auto stats_it = statistics_map.find(binding);
	if (stats_it != statistics_map.end()) {
		binding_info.stats = statistics_map[binding]->ToUnique();
	}
}

void CompressedMaterialization::Compress(unique_ptr<LogicalOperator> &op) {
	if (TopN::CanOptimize(*op)) { // Let's not mess with the TopN optimizer
		return;
	}

	switch (op->type) {
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY:
	case LogicalOperatorType::LOGICAL_DISTINCT:
	case LogicalOperatorType::LOGICAL_ORDER_BY:
		break;
	default:
		return;
	}

	root->ResolveOperatorTypes();

	switch (op->type) {
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY:
		CompressAggregate(op);
		break;
	case LogicalOperatorType::LOGICAL_DISTINCT:
		CompressDistinct(op);
		break;
	case LogicalOperatorType::LOGICAL_ORDER_BY:
		CompressOrder(op);
		break;
	default:
		return;
	}
}

void CompressedMaterialization::CreateProjections(unique_ptr<LogicalOperator> &op,
                                                  CompressedMaterializationInfo &info) {
	auto &materializing_op = *op;

	bool compressed_anything = false;
	for (idx_t i = 0; i < info.child_idxs.size(); i++) {
		auto &child_info = info.child_info[i];
		vector<unique_ptr<CompressExpression>> compress_exprs;
		if (TryCompressChild(info, child_info, compress_exprs)) {
			// We can compress: Create a projection on top of the child operator
			const auto child_idx = info.child_idxs[i];
			CreateCompressProjection(materializing_op.children[child_idx], std::move(compress_exprs), info, child_info);
			compressed_anything = true;
		}
	}

	if (compressed_anything) {
		CreateDecompressProjection(op, info);
	}
}

bool CompressedMaterialization::TryCompressChild(CompressedMaterializationInfo &info, const CMChildInfo &child_info,
                                                 vector<unique_ptr<CompressExpression>> &compress_exprs) {
	// Try to compress each of the column bindings of the child
	bool compressed_anything = false;
	for (idx_t child_i = 0; child_i < child_info.bindings_before.size(); child_i++) {
		const auto child_binding = child_info.bindings_before[child_i];
		const auto &child_type = child_info.types[child_i];
		const auto &can_compress = child_info.can_compress[child_i];
		auto compress_expr = GetCompressExpression(child_binding, child_type, can_compress);
		bool compressed = false;
		if (compress_expr) { // We compressed, mark the outgoing binding in need of decompression
			compress_exprs.emplace_back(std::move(compress_expr));
			compressed = true;
		} else { // We did not compress, just push a colref
			auto colref_expr = make_uniq<BoundColumnRefExpression>(child_type, child_binding);
			auto it = statistics_map.find(colref_expr->binding);
			unique_ptr<BaseStatistics> colref_stats = it != statistics_map.end() ? it->second->ToUnique() : nullptr;
			compress_exprs.emplace_back(make_uniq<CompressExpression>(std::move(colref_expr), std::move(colref_stats)));
		}
		UpdateBindingInfo(info, child_binding, compressed);
		compressed_anything = compressed_anything || compressed;
	}
	if (!compressed_anything) {
		// If we compressed anything non-generically, we still need to decompress
		for (const auto &entry : info.binding_map) {
			compressed_anything = compressed_anything || entry.second.needs_decompression;
		}
	}
	return compressed_anything;
}

void CompressedMaterialization::CreateCompressProjection(unique_ptr<LogicalOperator> &child_op,
                                                         vector<unique_ptr<CompressExpression>> compress_exprs,
                                                         CompressedMaterializationInfo &info, CMChildInfo &child_info) {
	// Replace child op with a projection
	vector<unique_ptr<Expression>> projections;
	projections.reserve(compress_exprs.size());
	for (auto &compress_expr : compress_exprs) {
		projections.emplace_back(std::move(compress_expr->expression));
	}
	const auto table_index = optimizer.binder.GenerateTableIndex();
	auto compress_projection = make_uniq<LogicalProjection>(table_index, std::move(projections));
	compress_projection->ResolveOperatorTypes();

	compress_projection->children.emplace_back(std::move(child_op));
	child_op = std::move(compress_projection);

	// Get the new bindings and types
	child_info.bindings_after = child_op->GetColumnBindings();
	const auto &new_types = child_op->types;

	// Initialize a ColumnBindingReplacer with the new bindings and types
	ColumnBindingReplacer replacer;
	auto &replacement_bindings = replacer.replacement_bindings;
	for (idx_t col_idx = 0; col_idx < child_info.bindings_before.size(); col_idx++) {
		const auto &old_binding = child_info.bindings_before[col_idx];
		const auto &new_binding = child_info.bindings_after[col_idx];
		const auto &new_type = new_types[col_idx];
		replacement_bindings.emplace_back(old_binding, new_binding, new_type);

		// Remove the old binding from the statistics map
		statistics_map.erase(old_binding);
	}

	// Make sure we stop at the compress operator when replacing bindings
	replacer.stop_operator = child_op.get();

	// Make the plan consistent again
	replacer.VisitOperator(*root);

	// Replace in/out exprs in the binding map too
	auto &binding_map = info.binding_map;
	for (auto &replacement_binding : replacement_bindings) {
		auto it = binding_map.find(replacement_binding.old_binding);
		if (it == binding_map.end()) {
			continue;
		}
		auto &binding_info = it->second;
		if (binding_info.binding == replacement_binding.old_binding) {
			binding_info.binding = replacement_binding.new_binding;
		}

		if (it->first == replacement_binding.old_binding) {
			auto binding_info_local = std::move(binding_info);
			binding_map.erase(it);
			binding_map.emplace(replacement_binding.new_binding, std::move(binding_info_local));
		}
	}

	// Add projection stats to statistics map
	for (idx_t col_idx = 0; col_idx < child_info.bindings_after.size(); col_idx++) {
		const auto &binding = child_info.bindings_after[col_idx];
		auto &stats = compress_exprs[col_idx]->stats;
		statistics_map.emplace(binding, std::move(stats));
	}
}

void CompressedMaterialization::CreateDecompressProjection(unique_ptr<LogicalOperator> &op,
                                                           CompressedMaterializationInfo &info) {
	const auto bindings = op->GetColumnBindings();
	op->ResolveOperatorTypes();
	const auto &types = op->types;

	// Create decompress expressions for everything we compressed
	auto &binding_map = info.binding_map;
	vector<unique_ptr<Expression>> decompress_exprs;
	vector<optional_ptr<BaseStatistics>> statistics;
	for (idx_t col_idx = 0; col_idx < bindings.size(); col_idx++) {
		const auto &binding = bindings[col_idx];
		auto decompress_expr = make_uniq_base<Expression, BoundColumnRefExpression>(types[col_idx], binding);
		optional_ptr<BaseStatistics> stats;
		for (auto &entry : binding_map) {
			auto &binding_info = entry.second;
			if (binding_info.binding != binding) {
				continue;
			}
			stats = binding_info.stats.get();
			if (binding_info.needs_decompression) {
				decompress_expr = GetDecompressExpression(std::move(decompress_expr), binding_info.type, *stats);
			}
		}
		statistics.push_back(stats);
		decompress_exprs.emplace_back(std::move(decompress_expr));
	}

	// Replace op with a projection
	const auto table_index = optimizer.binder.GenerateTableIndex();
	auto decompress_projection = make_uniq<LogicalProjection>(table_index, std::move(decompress_exprs));

	decompress_projection->children.emplace_back(std::move(op));
	op = std::move(decompress_projection);

	// Check if we're placing a projection on top of the root
	if (RefersToSameObject(*op->children[0], *root)) {
		root = op;
		return;
	}

	// Get the new bindings and types
	auto new_bindings = op->GetColumnBindings();
	op->ResolveOperatorTypes();
	auto &new_types = op->types;

	// Initialize a ColumnBindingReplacer with the new bindings and types
	ColumnBindingReplacer replacer;
	auto &replacement_bindings = replacer.replacement_bindings;
	for (idx_t col_idx = 0; col_idx < bindings.size(); col_idx++) {
		const auto &old_binding = bindings[col_idx];
		const auto &new_binding = new_bindings[col_idx];
		const auto &new_type = new_types[col_idx];
		replacement_bindings.emplace_back(old_binding, new_binding, new_type);

		if (statistics[col_idx]) {
			statistics_map[new_binding] = statistics[col_idx]->ToUnique();
		}
	}

	// Make sure we skip the decompress operator when replacing bindings
	replacer.stop_operator = op.get();

	// Make the plan consistent again
	replacer.VisitOperator(*root);
}

unique_ptr<CompressExpression> CompressedMaterialization::GetCompressExpression(const ColumnBinding &binding,
                                                                                const LogicalType &type,
                                                                                const bool &can_compress) {
	auto it = statistics_map.find(binding);
	if (can_compress && it != statistics_map.end() && it->second) {
		auto input = make_uniq<BoundColumnRefExpression>(type, binding);
		const auto &stats = *it->second;
		return GetCompressExpression(std::move(input), stats);
	}
	return nullptr;
}

unique_ptr<CompressExpression> CompressedMaterialization::GetCompressExpression(unique_ptr<Expression> input,
                                                                                const BaseStatistics &stats) {
	const auto &type = input->return_type;
	if (type != stats.GetType()) { // LCOV_EXCL_START
		return nullptr;
	} // LCOV_EXCL_STOP
	if (type.IsIntegral()) {
		return GetIntegralCompress(std::move(input), stats);
	} else if (type.id() == LogicalTypeId::VARCHAR) {
		return GetStringCompress(std::move(input), stats);
	}
	return nullptr;
}

static Value GetIntegralRangeValue(ClientContext &context, const LogicalType &type, const BaseStatistics &stats) {
	auto min = NumericStats::Min(stats);
	auto max = NumericStats::Max(stats);

	vector<unique_ptr<Expression>> arguments;
	arguments.emplace_back(make_uniq<BoundConstantExpression>(max));
	arguments.emplace_back(make_uniq<BoundConstantExpression>(min));
	BoundFunctionExpression sub(type, SubtractFun::GetFunction(type, type), std::move(arguments), nullptr);

	Value result;
	if (ExpressionExecutor::TryEvaluateScalar(context, sub, result)) {
		return result;
	} else {
		// Couldn't evaluate: Return max hugeint as range so GetIntegralCompress will return nullptr
		return Value::HUGEINT(NumericLimits<hugeint_t>::Maximum());
	}
}

unique_ptr<CompressExpression> CompressedMaterialization::GetIntegralCompress(unique_ptr<Expression> input,
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
	const auto range = UBigIntValue::Get(range_value);
	LogicalType cast_type;
	if (range <= NumericLimits<uint8_t>().Maximum()) {
		cast_type = LogicalType::UTINYINT;
	} else if (range <= NumericLimits<uint16_t>().Maximum()) {
		cast_type = LogicalType::USMALLINT;
	} else if (range <= NumericLimits<uint32_t>().Maximum()) {
		cast_type = LogicalType::UINTEGER;
	} else {
		D_ASSERT(range <= NumericLimits<uint64_t>().Maximum());
		cast_type = LogicalType::UBIGINT;
	}

	// Check if type that fits the range is smaller than the input type
	if (GetTypeIdSize(cast_type.InternalType()) == GetTypeIdSize(type.InternalType())) {
		return nullptr;
	}
	D_ASSERT(GetTypeIdSize(cast_type.InternalType()) < GetTypeIdSize(type.InternalType()));

	// Compressing will yield a benefit
	auto compress_function = CMIntegralCompressFun::GetFunction(type, cast_type);
	vector<unique_ptr<Expression>> arguments;
	arguments.emplace_back(std::move(input));
	arguments.emplace_back(make_uniq<BoundConstantExpression>(NumericStats::Min(stats)));
	auto compress_expr =
	    make_uniq<BoundFunctionExpression>(cast_type, compress_function, std::move(arguments), nullptr);

	auto compress_stats = BaseStatistics::CreateEmpty(cast_type);
	compress_stats.CopyBase(stats);
	NumericStats::SetMin(compress_stats, Value(0).DefaultCastAs(cast_type));
	NumericStats::SetMax(compress_stats, range_value.DefaultCastAs(cast_type));

	return make_uniq<CompressExpression>(std::move(compress_expr), compress_stats.ToUnique());
}

unique_ptr<CompressExpression> CompressedMaterialization::GetStringCompress(unique_ptr<Expression> input,
                                                                            const BaseStatistics &stats) {
	if (!StringStats::HasMaxStringLength(stats)) {
		return nullptr;
	}

	const auto max_string_length = StringStats::MaxStringLength(stats);
	LogicalType cast_type = LogicalType::INVALID;
	for (const auto &compressed_type : CompressedMaterializationFunctions::StringTypes()) {
		if (max_string_length < GetTypeIdSize(compressed_type.InternalType())) {
			cast_type = compressed_type;
			break;
		}
	}
	if (cast_type == LogicalType::INVALID) {
		return nullptr;
	}

	auto compress_stats = BaseStatistics::CreateEmpty(cast_type);
	compress_stats.CopyBase(stats);
	if (cast_type.id() == LogicalTypeId::USMALLINT) {
		auto min_string = StringStats::Min(stats);
		auto max_string = StringStats::Max(stats);

		uint8_t min_numeric = 0;
		if (max_string_length != 0 && !min_string.empty()) {
			min_numeric = *reinterpret_cast<const uint8_t *>(min_string.c_str());
		}
		uint8_t max_numeric = 0;
		if (max_string_length != 0 && !max_string.empty()) {
			max_numeric = *reinterpret_cast<const uint8_t *>(max_string.c_str());
		}

		Value min_val = Value::USMALLINT(min_numeric);
		Value max_val = Value::USMALLINT(max_numeric + 1);
		if (max_numeric < NumericLimits<uint8_t>::Maximum()) {
			cast_type = LogicalType::UTINYINT;
			compress_stats = BaseStatistics::CreateEmpty(cast_type);
			compress_stats.CopyBase(stats);
			min_val = Value::UTINYINT(min_numeric);
			max_val = Value::UTINYINT(max_numeric + 1);
		}

		NumericStats::SetMin(compress_stats, min_val);
		NumericStats::SetMax(compress_stats, max_val);
	}

	auto compress_function = CMStringCompressFun::GetFunction(cast_type);
	vector<unique_ptr<Expression>> arguments;
	arguments.emplace_back(std::move(input));
	auto compress_expr =
	    make_uniq<BoundFunctionExpression>(cast_type, compress_function, std::move(arguments), nullptr);
	return make_uniq<CompressExpression>(std::move(compress_expr), compress_stats.ToUnique());
}

unique_ptr<Expression> CompressedMaterialization::GetDecompressExpression(unique_ptr<Expression> input,
                                                                          const LogicalType &result_type,
                                                                          const BaseStatistics &stats) {
	const auto &type = result_type;
	if (TypeIsIntegral(type.InternalType())) {
		return GetIntegralDecompress(std::move(input), result_type, stats);
	} else if (type.id() == LogicalTypeId::VARCHAR) {
		return GetStringDecompress(std::move(input), result_type, stats);
	} else {
		throw InternalException("Type other than integral/string marked for decompression!");
	}
}

unique_ptr<Expression> CompressedMaterialization::GetIntegralDecompress(unique_ptr<Expression> input,
                                                                        const LogicalType &result_type,
                                                                        const BaseStatistics &stats) {
	D_ASSERT(NumericStats::HasMinMax(stats));
	auto decompress_function = CMIntegralDecompressFun::GetFunction(input->return_type, result_type);
	vector<unique_ptr<Expression>> arguments;
	arguments.emplace_back(std::move(input));
	arguments.emplace_back(make_uniq<BoundConstantExpression>(NumericStats::Min(stats)));
	return make_uniq<BoundFunctionExpression>(result_type, decompress_function, std::move(arguments), nullptr);
}

unique_ptr<Expression> CompressedMaterialization::GetStringDecompress(unique_ptr<Expression> input,
                                                                      const LogicalType &result_type,
                                                                      const BaseStatistics &stats) {
	D_ASSERT(StringStats::HasMaxStringLength(stats));
	auto decompress_function = CMStringDecompressFun::GetFunction(input->return_type);
	vector<unique_ptr<Expression>> arguments;
	arguments.emplace_back(std::move(input));
	return make_uniq<BoundFunctionExpression>(result_type, decompress_function, std::move(arguments), nullptr);
}

} // namespace duckdb
