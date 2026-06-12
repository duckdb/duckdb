#include "duckdb/optimizer/compressed_materialization.hpp"

#include "duckdb/common/exception/conversion_exception.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/scalar/compressed_materialization_utils.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/function/scalar/operators.hpp"
#include "duckdb/optimizer/column_binding_replacer.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/optimizer/topn_optimizer.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Utility
//===--------------------------------------------------------------------===//
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
    : binding(binding_p), type(type_p), materialization_type(CompressedMaterializationType::INVALID) {
}

CompressedMaterializationInfo::CompressedMaterializationInfo(LogicalOperator &op, vector<idx_t> &&child_idxs_p,
                                                             const column_binding_set_t &referenced_bindings)
    : child_idxs(std::move(child_idxs_p)) {
	child_info.reserve(child_idxs.size());
	for (const auto &child_idx : child_idxs) {
		child_info.emplace_back(*op.children[child_idx], referenced_bindings);
	}
}

CompressExpression::CompressExpression(unique_ptr<Expression> expression_p, unique_ptr<BaseStatistics> stats_p,
                                       CompressedMaterializationType materialization_type_p)
    : expression(std::move(expression_p)), stats(std::move(stats_p)), materialization_type(materialization_type_p) {
}

//===--------------------------------------------------------------------===//
// CMHelper hpp
//===--------------------------------------------------------------------===//
struct CMHelper {
	static bool TypeRequiresRestore(CompressedMaterializationType materialization_type);

	static unique_ptr<LogicalProjection> CreateProjection(const Optimizer &optimizer, const LogicalOperator &source,
	                                                      vector<unique_ptr<Expression>> projections);

	static void RemapBindingMap(CompressedMaterializationInfo &info,
	                            const vector<ReplacementBinding> &replacement_bindings);

	static void AddCompressProjectionStats(statistics_map_t &statistics_map, const vector<ColumnBinding> &bindings,
	                                       vector<unique_ptr<CompressExpression>> &compress_exprs);

	static Value GetIntegralRangeValue(ClientContext &context, const LogicalType &type, const BaseStatistics &stats);
	static LogicalType GetIntegralOffsetType(uint64_t range);
	static bool GetIntegralOffsetCompressInfo(ClientContext &context, const LogicalType &type,
	                                          const BaseStatistics &stats, LogicalType &offset_type, Value &min,
	                                          Value &range_value);
	static LogicalType GetSameWidthIntegralType(const LogicalType &type, bool use_signed);
	static bool ValuePreservingCastFits(const Value &value, const LogicalType &source_type,
	                                    const LogicalType &target_type);
	static LogicalType GetIntegralCastType(const LogicalType &source_type, const LogicalType &offset_type,
	                                       const BaseStatistics &stats);
	static unique_ptr<BaseStatistics> CreateIntegralCastStats(const LogicalType &target_type,
	                                                          const BaseStatistics &stats);
	static unique_ptr<CompressExpression> CreateIntegralCastCompress(ClientContext &context,
	                                                                 unique_ptr<Expression> input,
	                                                                 const LogicalType &target_type,
	                                                                 const BaseStatistics &stats);
	static unique_ptr<CompressExpression> CreateIntegralFunctionCompress(unique_ptr<Expression> input,
	                                                                     const LogicalType &source_type,
	                                                                     const LogicalType &target_type,
	                                                                     const Value &min, const Value &range_value,
	                                                                     const BaseStatistics &stats);

	static unique_ptr<BaseStatistics> CreateStringCompressStats(const BaseStatistics &stats, LogicalType &target_type,
	                                                            const uint32_t max_string_length);
	static bool GetStringCompressInfo(const BaseStatistics &stats, LogicalType &target_type,
	                                  uint32_t &max_string_length);
	static unique_ptr<CompressExpression> CreateStringFunctionCompress(unique_ptr<Expression> input,
	                                                                   const LogicalType &target_type,
	                                                                   unique_ptr<BaseStatistics> compress_stats);
};

//===--------------------------------------------------------------------===//
// CMHelper cpp
//===--------------------------------------------------------------------===//
bool CMHelper::TypeRequiresRestore(CompressedMaterializationType materialization_type) {
	return materialization_type != CompressedMaterializationType::INVALID;
}

unique_ptr<LogicalProjection> CMHelper::CreateProjection(const Optimizer &optimizer, const LogicalOperator &source,
                                                         vector<unique_ptr<Expression>> projections) {
	const auto table_index = optimizer.binder.GenerateTableIndex();
	auto projection = make_uniq<LogicalProjection>(table_index, std::move(projections));
	if (source.has_estimated_cardinality) {
		projection->SetEstimatedCardinality(source.estimated_cardinality);
	}
	return projection;
}

void CMHelper::RemapBindingMap(CompressedMaterializationInfo &info,
                               const vector<ReplacementBinding> &replacement_bindings) {
	auto &binding_map = info.binding_map;
	for (const auto &replacement_binding : replacement_bindings) {
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
}

void CMHelper::AddCompressProjectionStats(statistics_map_t &statistics_map, const vector<ColumnBinding> &bindings,
                                          vector<unique_ptr<CompressExpression>> &compress_exprs) {
	for (idx_t col_idx = 0; col_idx < bindings.size(); col_idx++) {
		const auto &binding = bindings[col_idx];
		auto &stats = compress_exprs[col_idx]->stats;
		statistics_map.emplace(binding, std::move(stats));
	}
}

Value CMHelper::GetIntegralRangeValue(ClientContext &context, const LogicalType &type, const BaseStatistics &stats) {
	auto min = NumericStats::Min(stats);
	auto max = NumericStats::Max(stats);
	if (max < min) {
		return Value::UHUGEINT(NumericLimits<uhugeint_t>::Maximum());
	}

	vector<unique_ptr<Expression>> arguments;
	arguments.emplace_back(make_uniq<BoundConstantExpression>(max));
	arguments.emplace_back(make_uniq<BoundConstantExpression>(min));

	auto sub = SubtractFunction::GetFunction(type, type).Bind(context, std::move(arguments));

	Value result;
	if (ExpressionExecutor::TryEvaluateScalar(context, *sub, result)) {
		return result;
	}
	// Couldn't evaluate: Return max uhugeint as range so GetIntegralCompress will return nullptr
	return Value::UHUGEINT(NumericLimits<uhugeint_t>::Maximum());
}

LogicalType CMHelper::GetIntegralOffsetType(const uint64_t range) {
	if (range <= NumericLimits<uint8_t>().Maximum()) {
		return LogicalType::UTINYINT;
	}
	if (range <= NumericLimits<uint16_t>().Maximum()) {
		return LogicalType::USMALLINT;
	}
	if (range <= NumericLimits<uint32_t>().Maximum()) {
		return LogicalType::UINTEGER;
	}
	D_ASSERT(range <= NumericLimits<uint64_t>().Maximum());
	return LogicalType::UBIGINT;
}

bool CMHelper::GetIntegralOffsetCompressInfo(ClientContext &context, const LogicalType &type,
                                             const BaseStatistics &stats, LogicalType &offset_type, Value &min,
                                             Value &range_value) {
	if (!stats.CanHaveNoNull()) {
		// All NULL
		offset_type = LogicalType::UTINYINT;
		range_value = Value::UTINYINT(0);
		min = Value(type);
		return true;
	}
	if (!NumericStats::HasMinMax(stats)) {
		return false;
	}

	// Get range and cast to UBIGINT (might fail for HUGEINT, in which case we just return)
	range_value = GetIntegralRangeValue(context, type, stats);
	if (!range_value.DefaultTryCastAs(LogicalType::UBIGINT)) {
		return false;
	}
	offset_type = GetIntegralOffsetType(UBigIntValue::Get(range_value));
	min = NumericStats::Min(stats);
	return true;
}

LogicalType CMHelper::GetSameWidthIntegralType(const LogicalType &type, const bool use_signed) {
	switch (GetTypeIdSize(type.InternalType())) {
	case 1:
		return use_signed ? LogicalType::TINYINT : LogicalType::UTINYINT;
	case 2:
		return use_signed ? LogicalType::SMALLINT : LogicalType::USMALLINT;
	case 4:
		return use_signed ? LogicalType::INTEGER : LogicalType::UINTEGER;
	case 8:
		return use_signed ? LogicalType::BIGINT : LogicalType::UBIGINT;
	default:
		return LogicalType::INVALID;
	}
}

bool CMHelper::ValuePreservingCastFits(const Value &value, const LogicalType &source_type,
                                       const LogicalType &target_type) {
	Value cast_value;
	Value roundtrip_value;
	try {
		if (!value.DefaultTryCastAs(target_type, cast_value, nullptr, true)) {
			return false;
		}
		if (!cast_value.DefaultTryCastAs(source_type, roundtrip_value, nullptr, true)) {
			return false;
		}
	} catch (ConversionException &) {
		return false;
	}
	return value == roundtrip_value;
}

LogicalType CMHelper::GetIntegralCastType(const LogicalType &source_type, const LogicalType &offset_type,
                                          const BaseStatistics &stats) {
	if (GetTypeIdSize(source_type.InternalType()) <= GetTypeIdSize(offset_type.InternalType())) {
		return LogicalType::INVALID;
	}

	const auto preferred_signed = source_type.IsSigned();
	auto preferred_type = GetSameWidthIntegralType(offset_type, preferred_signed);
	if (!stats.CanHaveNoNull()) {
		return preferred_type;
	}
	if (!NumericStats::HasMinMax(stats)) {
		return LogicalType::INVALID;
	}

	auto alternate_type = GetSameWidthIntegralType(offset_type, !preferred_signed);
	const auto min = NumericStats::Min(stats);
	const auto max = NumericStats::Max(stats);
	if (preferred_type.IsValid() && ValuePreservingCastFits(min, source_type, preferred_type) &&
	    ValuePreservingCastFits(max, source_type, preferred_type)) {
		return preferred_type;
	}
	if (alternate_type.IsValid() && ValuePreservingCastFits(min, source_type, alternate_type) &&
	    ValuePreservingCastFits(max, source_type, alternate_type)) {
		return alternate_type;
	}
	return LogicalType::INVALID;
}

unique_ptr<BaseStatistics> CMHelper::CreateIntegralCastStats(const LogicalType &target_type,
                                                             const BaseStatistics &stats) {
	auto compress_stats = BaseStatistics::CreateEmpty(target_type);
	compress_stats.CopyBase(stats);
	if (NumericStats::HasMinMax(stats)) {
		Value cast_min;
		Value cast_max;
		const auto min_success = NumericStats::Min(stats).DefaultTryCastAs(target_type, cast_min, nullptr, true);
		const auto max_success = NumericStats::Max(stats).DefaultTryCastAs(target_type, cast_max, nullptr, true);
		if (!min_success || !max_success) {
			throw InternalException("Casting failure in CMHelper::CreateIntegralCastStats");
		}
		NumericStats::SetMin(compress_stats, cast_min);
		NumericStats::SetMax(compress_stats, cast_max);
	}
	return compress_stats.ToUnique();
}

unique_ptr<CompressExpression> CMHelper::CreateIntegralCastCompress(ClientContext &context,
                                                                    unique_ptr<Expression> input,
                                                                    const LogicalType &target_type,
                                                                    const BaseStatistics &stats) {
	auto compress_expr = BoundCastExpression::AddCastToType(context, std::move(input), target_type);
	auto compress_stats = CreateIntegralCastStats(target_type, stats);
	return make_uniq<CompressExpression>(std::move(compress_expr), std::move(compress_stats),
	                                     CompressedMaterializationType::CAST);
}

unique_ptr<CompressExpression> CMHelper::CreateIntegralFunctionCompress(unique_ptr<Expression> input,
                                                                        const LogicalType &source_type,
                                                                        const LogicalType &target_type,
                                                                        const Value &min, const Value &range_value,
                                                                        const BaseStatistics &stats) {
	auto compress_function = CMIntegralCompressFun::GetFunction(source_type, target_type);
	vector<unique_ptr<Expression>> arguments;
	arguments.emplace_back(std::move(input));
	arguments.emplace_back(make_uniq<BoundConstantExpression>(min));

	BoundScalarFunction bound_function(compress_function);
	bound_function.SetReturnType(target_type);
	auto compress_expr = make_uniq<BoundFunctionExpression>(std::move(bound_function), std::move(arguments), nullptr);

	auto compress_stats = BaseStatistics::CreateEmpty(target_type);
	compress_stats.CopyBase(stats);
	NumericStats::SetMin(compress_stats, Value(0).DefaultCastAs(target_type));
	NumericStats::SetMax(compress_stats, range_value.DefaultCastAs(target_type));

	return make_uniq<CompressExpression>(std::move(compress_expr), compress_stats.ToUnique(),
	                                     CompressedMaterializationType::FUNCTION);
}

//===--------------------------------------------------------------------===//
// CompressedMaterialization
//===--------------------------------------------------------------------===//
CompressedMaterialization::CompressedMaterialization(Optimizer &optimizer_p, LogicalOperator &root_p,
                                                     statistics_map_t &statistics_map_p)
    : optimizer(optimizer_p), context(optimizer.context), root(&root_p), statistics_map(statistics_map_p) {
}

void CompressedMaterialization::GetReferencedBindings(const Expression &root_expr,
                                                      column_binding_set_t &referenced_bindings) {
	ExpressionIterator::VisitExpression<BoundColumnRefExpression>(
	    root_expr, [&](const BoundColumnRefExpression &col_ref) { referenced_bindings.insert(col_ref.Binding()); });
}

void CompressedMaterialization::UpdateBindingInfo(CompressedMaterializationInfo &info, const ColumnBinding &binding,
                                                  CompressedMaterializationType materialization_type) {
	auto &binding_map = info.binding_map;
	auto binding_it = binding_map.find(binding);
	if (binding_it == binding_map.end()) {
		return;
	}

	auto &binding_info = binding_it->second;
	binding_info.materialization_type = materialization_type;
	if (!binding_info.stats) {
		auto stats_it = statistics_map.find(binding);
		if (stats_it != statistics_map.end() && stats_it->second) {
			binding_info.stats = stats_it->second->ToUnique();
		}
	}
}

void CompressedMaterialization::Compress(unique_ptr<LogicalOperator> &op) {
	if (TopN::CanOptimize(*op)) { // Let's not mess with the TopN optimizer
		return;
	}

	switch (op->type) {
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY:
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
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
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
		CompressComparisonJoin(op);
		break;
	case LogicalOperatorType::LOGICAL_DISTINCT:
		CompressDistinct(op);
		break;
	case LogicalOperatorType::LOGICAL_ORDER_BY:
		CompressOrder(op);
		break;
	default:
		break;
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
		if (!compress_expr) {
			auto it = statistics_map.find(child_binding);
			auto colref_stats = it != statistics_map.end() ? it->second->ToUnique() : nullptr;
			auto colref_expr = make_uniq<BoundColumnRefExpression>(child_type, child_binding);
			compress_expr = make_uniq<CompressExpression>(std::move(colref_expr), std::move(colref_stats),
			                                              CompressedMaterializationType::INVALID);
		}
		const auto materialization_type = compress_expr->materialization_type;
		compress_exprs.emplace_back(std::move(compress_expr));
		UpdateBindingInfo(info, child_binding, materialization_type);
		compressed_anything = compressed_anything || CMHelper::TypeRequiresRestore(materialization_type);
	}
	if (!compressed_anything) {
		// If we compressed anything non-generically, we still need to decompress
		for (const auto &entry : info.binding_map) {
			compressed_anything =
			    compressed_anything || CMHelper::TypeRequiresRestore(entry.second.materialization_type);
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
	auto compress_projection = CMHelper::CreateProjection(optimizer, *child_op, std::move(projections));
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
	CMHelper::RemapBindingMap(info, replacement_bindings);

	// Add projection stats to statistics map
	CMHelper::AddCompressProjectionStats(statistics_map, child_info.bindings_after, compress_exprs);
}

unique_ptr<Expression> CompressedMaterialization::CreateRestoreExpression(unique_ptr<Expression> input,
                                                                          const CMBindingInfo &binding_info,
                                                                          const BaseStatistics &stats) {
	switch (binding_info.materialization_type) {
	case CompressedMaterializationType::INVALID:
		return input;
	case CompressedMaterializationType::FUNCTION:
		return GetDecompressExpression(std::move(input), binding_info.type, stats);
	case CompressedMaterializationType::CAST:
		return BoundCastExpression::AddCastToType(context, std::move(input), binding_info.type);
	default:
		throw InternalException("Invalid compressed materialization type");
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
			if (CMHelper::TypeRequiresRestore(binding_info.materialization_type)) {
				decompress_expr = CreateRestoreExpression(std::move(decompress_expr), binding_info, *stats);
			}
		}
		statistics.push_back(stats);
		decompress_exprs.emplace_back(std::move(decompress_expr));
	}

	// Replace op with a projection
	auto decompress_projection = CMHelper::CreateProjection(optimizer, *op, std::move(decompress_exprs));

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
	const auto &type = input->GetReturnType();
	if (type.IsAggregateState()) {
		return nullptr;
	}
	if (type != stats.GetType()) { // LCOV_EXCL_START
		return nullptr;
	} // LCOV_EXCL_STOP
	if (type.IsIntegral()) {
		return GetIntegralCompress(std::move(input), stats);
	}
	if (type.id() == LogicalTypeId::VARCHAR) {
		return GetStringCompress(std::move(input), stats);
	}
	return nullptr;
}

unique_ptr<CompressExpression> CompressedMaterialization::GetIntegralCompress(unique_ptr<Expression> input,
                                                                              const BaseStatistics &stats) {
	const auto &type = input->GetReturnType();
	if (GetTypeIdSize(type.InternalType()) == 1) {
		return nullptr;
	}

	LogicalType cast_type;
	Value range_value;
	Value min;
	if (!CMHelper::GetIntegralOffsetCompressInfo(context, type, stats, cast_type, min, range_value)) {
		// We don't have enough stats to do anything
		return nullptr;
	}

	// Check if type that fits the range is smaller than the input type
	if (GetTypeIdSize(cast_type.InternalType()) == GetTypeIdSize(type.InternalType())) {
		return nullptr;
	}
	D_ASSERT(GetTypeIdSize(cast_type.InternalType()) < GetTypeIdSize(type.InternalType()));

	const auto value_preserving_cast_type = CMHelper::GetIntegralCastType(type, cast_type, stats);
	if (value_preserving_cast_type.IsValid()) {
		return CMHelper::CreateIntegralCastCompress(context, std::move(input), value_preserving_cast_type, stats);
	}

	return CMHelper::CreateIntegralFunctionCompress(std::move(input), type, cast_type, min, range_value, stats);
}

unique_ptr<BaseStatistics> CMHelper::CreateStringCompressStats(const BaseStatistics &stats, LogicalType &target_type,
                                                               const uint32_t max_string_length) {
	auto compress_stats = BaseStatistics::CreateEmpty(target_type);
	compress_stats.CopyBase(stats);
	if (target_type.id() != LogicalTypeId::USMALLINT || !StringStats::HasMinMax(stats)) {
		return compress_stats.ToUnique();
	}

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
		target_type = LogicalType::UTINYINT;
		compress_stats = BaseStatistics::CreateEmpty(target_type);
		compress_stats.CopyBase(stats);
		min_val = Value::UTINYINT(min_numeric);
		max_val = Value::UTINYINT(max_numeric + 1);
	}

	NumericStats::SetMin(compress_stats, min_val);
	NumericStats::SetMax(compress_stats, max_val);
	return compress_stats.ToUnique();
}

bool CMHelper::GetStringCompressInfo(const BaseStatistics &stats, LogicalType &target_type,
                                     uint32_t &max_string_length) {
	if (!stats.CanHaveNoNull()) {
		// All NULL
		target_type = LogicalType::UTINYINT;
		max_string_length = 0;
		return true;
	}
	if (!StringStats::HasMaxStringLength(stats)) {
		return false;
	}

	max_string_length = StringStats::MaxStringLength(stats);
	for (const auto &compressed_type : CMUtils::StringTypes()) {
		if (max_string_length < GetTypeIdSize(compressed_type.InternalType())) {
			target_type = compressed_type;
			return true;
		}
	}
	return false;
}

unique_ptr<CompressExpression> CMHelper::CreateStringFunctionCompress(unique_ptr<Expression> input,
                                                                      const LogicalType &target_type,
                                                                      unique_ptr<BaseStatistics> compress_stats) {
	auto compress_function = CMStringCompressFun::GetFunction(target_type);
	vector<unique_ptr<Expression>> arguments;
	arguments.emplace_back(std::move(input));

	BoundScalarFunction bound_function(compress_function);
	bound_function.SetReturnType(target_type);

	auto compress_expr = make_uniq<BoundFunctionExpression>(std::move(bound_function), std::move(arguments), nullptr);
	return make_uniq<CompressExpression>(std::move(compress_expr), std::move(compress_stats),
	                                     CompressedMaterializationType::FUNCTION);
}

unique_ptr<CompressExpression> CompressedMaterialization::GetStringCompress(unique_ptr<Expression> input,
                                                                            const BaseStatistics &stats) {
	LogicalType cast_type = LogicalType::INVALID;
	uint32_t max_string_length = 0;
	if (!CMHelper::GetStringCompressInfo(stats, cast_type, max_string_length)) {
		// We don't have enough stats to do anything
		return nullptr;
	}
	auto compress_stats = CMHelper::CreateStringCompressStats(stats, cast_type, max_string_length);
	return CMHelper::CreateStringFunctionCompress(std::move(input), cast_type, std::move(compress_stats));
}

unique_ptr<Expression> CompressedMaterialization::GetDecompressExpression(unique_ptr<Expression> input,
                                                                          const LogicalType &result_type,
                                                                          const BaseStatistics &stats) {
	const auto &type = result_type;
	if (TypeIsIntegral(type.InternalType())) {
		return GetIntegralDecompress(std::move(input), result_type, stats);
	}
	if (type.id() == LogicalTypeId::VARCHAR) {
		return GetStringDecompress(std::move(input), result_type, stats);
	}
	throw InternalException("Type other than integral/string marked for decompression!");
}

unique_ptr<Expression> CompressedMaterialization::GetIntegralDecompress(unique_ptr<Expression> input,
                                                                        const LogicalType &result_type,
                                                                        const BaseStatistics &stats) {
	D_ASSERT(!stats.CanHaveNoNull() || NumericStats::HasMinMax(stats));
	auto decompress_function = CMIntegralDecompressFun::GetFunction(input->GetReturnType(), result_type);
	const auto min = !stats.CanHaveNoNull() ? Value(result_type) : NumericStats::Min(stats);
	vector<unique_ptr<Expression>> arguments;
	arguments.emplace_back(std::move(input));
	arguments.emplace_back(make_uniq<BoundConstantExpression>(min));

	BoundScalarFunction bound_function(decompress_function);
	bound_function.SetReturnType(result_type);

	return make_uniq<BoundFunctionExpression>(std::move(bound_function), std::move(arguments), nullptr);
}

unique_ptr<Expression> CompressedMaterialization::GetStringDecompress(unique_ptr<Expression> input,
                                                                      const LogicalType &result_type,
                                                                      const BaseStatistics &stats) {
	D_ASSERT(!stats.CanHaveNoNull() || StringStats::HasMaxStringLength(stats));
	auto decompress_function = CMStringDecompressFun::GetFunction(input->GetReturnType());
	vector<unique_ptr<Expression>> arguments;
	arguments.emplace_back(std::move(input));

	BoundScalarFunction bound_function(decompress_function);
	bound_function.SetReturnType(result_type);

	return make_uniq<BoundFunctionExpression>(std::move(bound_function), std::move(arguments), nullptr);
}

} // namespace duckdb
