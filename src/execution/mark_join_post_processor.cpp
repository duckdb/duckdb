//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/mark_join_post_processor.cpp
//
//
//===----------------------------------------------------------------------===//

#include "duckdb/execution/mark_join_post_processor.hpp"

#include "duckdb/common/vector/vector_iterator.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/function/aggregate/distributive_function_utils.hpp"
#include "duckdb/function/aggregate/distributive_functions.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

namespace duckdb {

namespace {

using MarkJoinNullRemainder = MarkJoinPostProcessor::MarkJoinNullRemainder;

void InitializeMarkJoinNullRemainder(MarkJoinNullRemainder &remainder, BufferManager &buffer_manager,
                                     const vector<LogicalType> &key_types) {
	if (remainder.data) {
		return;
	}
	remainder.key_types = key_types;
	remainder.layout = make_shared_ptr<TupleDataLayout>();
	remainder.layout->Initialize(remainder.key_types, TupleDataValidityType::CAN_HAVE_NULL_VALUES);
	remainder.data = make_uniq<TupleDataCollection>(buffer_manager, remainder.layout, MemoryTag::HASH_TABLE);
	remainder.data->InitializeAppend(remainder.append_state);
}

idx_t BuildUnresolvedSelection(const bool *found_match, ValidityMask &mask, idx_t count,
                               SelectionVector &unresolved_sel) {
	idx_t unresolved_count = 0;
	for (idx_t row_idx = 0; row_idx < count; row_idx++) {
		if (!found_match[row_idx] && mask.RowIsValid(row_idx)) {
			unresolved_sel.set_index(unresolved_count++, NumericCast<sel_t>(row_idx));
		}
	}
	return unresolved_count;
}

void InvalidateSelection(ValidityMask &mask, const SelectionVector &sel, idx_t count) {
	for (idx_t i = 0; i < count; i++) {
		mask.SetInvalid(sel.get_index(i));
	}
}

idx_t CompactUnresolvedSelection(const SelectionVector &unresolved_sel, idx_t unresolved_count,
                                 const SelectionVector &matched_sel, idx_t matched_count,
                                 SelectionVector &remaining_sel) {
	idx_t matched_idx = 0;
	idx_t remaining_count = 0;
	for (idx_t i = 0; i < unresolved_count; i++) {
		const auto row_idx = unresolved_sel.get_index(i);
		while (matched_idx < matched_count && matched_sel.get_index(matched_idx) < row_idx) {
			matched_idx++;
		}
		if (matched_idx < matched_count && matched_sel.get_index(matched_idx) == row_idx) {
			continue;
		}
		remaining_sel.set_index(remaining_count++, row_idx);
	}
	return remaining_count;
}

struct MarkJoinNullMatchState {
	SelectionVector equal_sel;
	SelectionVector unequal_sel;
	SelectionVector candidate_sel_a;
	SelectionVector candidate_sel_b;
	SelectionVector row_match_sel;
	ValidityMask null_mask;
	ValidityMask matched_mask;
	vector<Vector> rhs_constant_values;
	idx_t probe_count;

	MarkJoinNullMatchState(idx_t probe_count, const vector<LogicalType> &condition_types)
	    : equal_sel(STANDARD_VECTOR_SIZE), unequal_sel(STANDARD_VECTOR_SIZE), candidate_sel_a(STANDARD_VECTOR_SIZE),
	      candidate_sel_b(STANDARD_VECTOR_SIZE), row_match_sel(STANDARD_VECTOR_SIZE), null_mask(probe_count),
	      matched_mask(probe_count), probe_count(probe_count) {
		matched_mask.SetAllInvalid(probe_count);
		rhs_constant_values.reserve(condition_types.size());
		for (auto &type : condition_types) {
			rhs_constant_values.emplace_back(type);
		}
	}

	void ResetMatchedMask() {
		matched_mask.SetAllInvalid(probe_count);
	}
};

idx_t FilterCandidatesForColumnComparison(Vector &lhs_column, Vector &rhs_column, Vector &rhs_value, idx_t rhs_row,
                                          idx_t rhs_count, const SelectionVector &candidate_sel, idx_t candidate_count,
                                          MarkJoinNullMatchState &state, SelectionVector &remaining_sel) {
	auto rhs_scalar = rhs_column.GetValue(rhs_row);
	if (rhs_scalar.type() != rhs_value.GetType()) {
		rhs_scalar = rhs_scalar.DefaultCastAs(rhs_value.GetType());
	}
	if (rhs_scalar.IsNull()) {
		ConstantVector::SetNull(rhs_value, count_t(candidate_count));
	} else {
		ConstantVector::Reference(rhs_value, rhs_scalar, count_t(candidate_count));
	}
	Vector lhs_slice(lhs_column, candidate_sel, candidate_count);

	state.null_mask.SetAllValid(candidate_count);
	const idx_t equal_count = VectorOperations::Equals(lhs_slice, rhs_value, nullptr, candidate_count, &state.equal_sel,
	                                                   &state.unequal_sel, &state.null_mask);

	idx_t remaining_count = 0;
	for (idx_t i = 0; i < equal_count; i++) {
		auto local_idx = state.equal_sel.get_index(i);
		remaining_sel.set_index(remaining_count++, candidate_sel.get_index(local_idx));
	}
	for (idx_t i = 0; i < candidate_count - equal_count; i++) {
		const auto local_idx = state.unequal_sel.get_index(i);
		if (!state.null_mask.RowIsValid(local_idx)) {
			remaining_sel.set_index(remaining_count++, candidate_sel.get_index(local_idx));
		}
	}
	return remaining_count;
}

idx_t MatchRemainderRow(DataChunk &join_keys, DataChunk &scan_chunk, idx_t scan_row,
                        const vector<VectorValidityIterator> &rhs_validities, const SelectionVector &unresolved_sel,
                        idx_t unresolved_count, MarkJoinNullMatchState &state) {
	reference<const SelectionVector> candidate_sel(unresolved_sel);
	idx_t candidate_count = unresolved_count;
	bool use_a = true;

	for (idx_t col_idx = 0; col_idx < scan_chunk.ColumnCount(); col_idx++) {
		if (!rhs_validities[col_idx].IsValid(scan_row)) {
			continue;
		}
		auto &remaining_sel = use_a ? state.candidate_sel_a : state.candidate_sel_b;
		candidate_count = FilterCandidatesForColumnComparison(
		    join_keys.data[col_idx], scan_chunk.data[col_idx], state.rhs_constant_values[col_idx], scan_row,
		    scan_chunk.size(), candidate_sel.get(), candidate_count, state, remaining_sel);
		candidate_sel = remaining_sel;
		use_a = !use_a;
		if (candidate_count == 0) {
			break;
		}
	}

	for (idx_t i = 0; i < candidate_count; i++) {
		state.row_match_sel.set_index(i, candidate_sel.get().get_index(i));
	}
	return candidate_count;
}

idx_t MatchNullRemainderChunk(DataChunk &join_keys, const SelectionVector &unresolved_sel, idx_t unresolved_count,
                              DataChunk &scan_chunk, SelectionVector &matched_sel, MarkJoinNullMatchState &state) {
	state.ResetMatchedMask();
	vector<VectorValidityIterator> rhs_validities;
	rhs_validities.reserve(scan_chunk.ColumnCount());
	for (idx_t col_idx = 0; col_idx < scan_chunk.ColumnCount(); col_idx++) {
		rhs_validities.emplace_back(scan_chunk.data[col_idx]);
	}

	for (idx_t scan_row = 0; scan_row < scan_chunk.size(); scan_row++) {
		const idx_t row_match_count =
		    MatchRemainderRow(join_keys, scan_chunk, scan_row, rhs_validities, unresolved_sel, unresolved_count, state);
		for (idx_t i = 0; i < row_match_count; i++) {
			state.matched_mask.SetValid(state.row_match_sel.get_index(i));
		}
	}

	idx_t matched_count = 0;
	for (idx_t i = 0; i < unresolved_count; i++) {
		const auto row_idx = unresolved_sel.get_index(i);
		if (state.matched_mask.RowIsValid(row_idx)) {
			matched_sel.set_index(matched_count++, row_idx);
		}
	}
	return matched_count;
}

bool ComparisonPropagatesNull(ExpressionType comparison_type) {
	switch (comparison_type) {
	case ExpressionType::COMPARE_DISTINCT_FROM:
	case ExpressionType::COMPARE_NOT_DISTINCT_FROM:
		return false;
	default:
		return true;
	}
}

bool IsUnnamedStructType(const LogicalType &type) {
	return type.id() == LogicalTypeId::STRUCT && type.InternalType() == PhysicalType::STRUCT &&
	       StructType::IsUnnamed(type);
}

static bool MarkJoinValueContainsNestedNull(const Value &value) {
	if (value.IsNull()) {
		return true;
	}
	if (value.type().id() == LogicalTypeId::UNION) {
		return MarkJoinValueContainsNestedNull(UnionValue::GetValue(value));
	}
	if (value.type().InternalType() != PhysicalType::STRUCT) {
		return false;
	}
	for (auto &child : StructValue::GetChildren(value)) {
		if (MarkJoinValueContainsNestedNull(child)) {
			return true;
		}
	}
	return false;
}

enum class NestedEqualityResult : uint8_t { FALSE_VALUE, TRUE_VALUE, NULL_VALUE };

static NestedEqualityResult EvaluateNestedEqualityForMarkJoin(const Value &left, const Value &right) {
	if (left.IsNull() || right.IsNull()) {
		return NestedEqualityResult::NULL_VALUE;
	}
	if (left.type().id() == LogicalTypeId::UNION && right.type().id() == LogicalTypeId::UNION) {
		if (UnionValue::GetTag(left) != UnionValue::GetTag(right)) {
			return NestedEqualityResult::FALSE_VALUE;
		}
		return EvaluateNestedEqualityForMarkJoin(UnionValue::GetValue(left), UnionValue::GetValue(right));
	}
	auto &left_children = StructValue::GetChildren(left);
	auto &right_children = StructValue::GetChildren(right);
	D_ASSERT(left_children.size() == right_children.size());
	bool has_unknown = false;
	for (idx_t i = 0; i < left_children.size(); i++) {
		auto &lhs_child = left_children[i];
		auto &rhs_child = right_children[i];
		if ((lhs_child.type().InternalType() == PhysicalType::STRUCT ||
		     lhs_child.type().id() == LogicalTypeId::UNION) &&
		    (rhs_child.type().InternalType() == PhysicalType::STRUCT ||
		     rhs_child.type().id() == LogicalTypeId::UNION)) {
			auto child_result = EvaluateNestedEqualityForMarkJoin(lhs_child, rhs_child);
			if (child_result == NestedEqualityResult::FALSE_VALUE) {
				return NestedEqualityResult::FALSE_VALUE;
			}
			has_unknown = has_unknown || child_result == NestedEqualityResult::NULL_VALUE;
			continue;
		}
		if (lhs_child.IsNull() || rhs_child.IsNull()) {
			has_unknown = true;
			continue;
		}
		if (!ValueOperations::NotDistinctFrom(lhs_child, rhs_child)) {
			return NestedEqualityResult::FALSE_VALUE;
		}
	}
	return has_unknown ? NestedEqualityResult::NULL_VALUE : NestedEqualityResult::TRUE_VALUE;
}

idx_t SelectNestedEqualsOrNotEqualsForMarkJoin(const Vector &left, const Vector &right,
                                               optional_ptr<const SelectionVector> sel, idx_t count,
                                               optional_ptr<SelectionVector> true_sel,
                                               optional_ptr<SelectionVector> false_sel,
                                               optional_ptr<ValidityMask> null_mask, bool invert) {
	idx_t true_count = 0;
	idx_t false_count = 0;
	for (idx_t i = 0; i < count; i++) {
		const auto result_idx = sel ? sel->get_index(i) : i;
		auto row_result = EvaluateNestedEqualityForMarkJoin(left.GetValue(result_idx), right.GetValue(result_idx));
		if (row_result == NestedEqualityResult::NULL_VALUE) {
			if (null_mask) {
				null_mask->SetInvalid(result_idx);
			}
			if (false_sel) {
				false_sel->set_index(false_count++, result_idx);
			}
			continue;
		}
		const bool matches = row_result == NestedEqualityResult::TRUE_VALUE ? !invert : invert;
		if (matches) {
			if (true_sel) {
				true_sel->set_index(true_count, result_idx);
			}
			true_count++;
		} else {
			if (false_sel) {
				false_sel->set_index(false_count++, result_idx);
			}
		}
	}
	return true_count;
}

idx_t SelectComparison(ExpressionType comparison_type, const Vector &left, const Vector &right,
                       optional_ptr<const SelectionVector> sel, idx_t count, optional_ptr<SelectionVector> true_sel,
                       optional_ptr<SelectionVector> false_sel, optional_ptr<ValidityMask> null_mask) {
	switch (comparison_type) {
	case ExpressionType::COMPARE_EQUAL:
		if (left.GetType().id() == LogicalTypeId::STRUCT && right.GetType().id() == LogicalTypeId::STRUCT &&
		    left.GetType().InternalType() == PhysicalType::STRUCT &&
		    right.GetType().InternalType() == PhysicalType::STRUCT && StructType::IsUnnamed(left.GetType()) &&
		    StructType::IsUnnamed(right.GetType())) {
			return SelectNestedEqualsOrNotEqualsForMarkJoin(left, right, sel, count, true_sel, false_sel, null_mask,
			                                                false);
		}
		return VectorOperations::Equals(left, right, sel, count, true_sel, false_sel, null_mask);
	case ExpressionType::COMPARE_NOTEQUAL:
		if (left.GetType().id() == LogicalTypeId::STRUCT && right.GetType().id() == LogicalTypeId::STRUCT &&
		    left.GetType().InternalType() == PhysicalType::STRUCT &&
		    right.GetType().InternalType() == PhysicalType::STRUCT && StructType::IsUnnamed(left.GetType()) &&
		    StructType::IsUnnamed(right.GetType())) {
			return SelectNestedEqualsOrNotEqualsForMarkJoin(left, right, sel, count, true_sel, false_sel, null_mask,
			                                                true);
		}
		return VectorOperations::NotEquals(left, right, sel, count, true_sel, false_sel, null_mask);
	case ExpressionType::COMPARE_LESSTHAN:
		return VectorOperations::LessThan(left, right, sel, count, true_sel, false_sel, null_mask);
	case ExpressionType::COMPARE_GREATERTHAN:
		return VectorOperations::GreaterThan(left, right, sel, count, true_sel, false_sel, null_mask);
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		return VectorOperations::LessThanEquals(left, right, sel, count, true_sel, false_sel, null_mask);
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		return VectorOperations::GreaterThanEquals(left, right, sel, count, true_sel, false_sel, null_mask);
	case ExpressionType::COMPARE_DISTINCT_FROM:
		return VectorOperations::DistinctFrom(left, right, sel, count, true_sel, false_sel);
	case ExpressionType::COMPARE_NOT_DISTINCT_FROM:
		return VectorOperations::NotDistinctFrom(left, right, sel, count, true_sel, false_sel);
	default:
		throw InternalException("Unsupported comparison type for MARK null refinement");
	}
}

idx_t FilterCandidatesForConditionComparison(Vector &lhs_column, Vector &rhs_column, Vector &rhs_value, idx_t rhs_row,
                                             idx_t rhs_count, const SelectionVector &candidate_sel,
                                             idx_t candidate_count, ExpressionType comparison_type,
                                             MarkJoinNullMatchState &state, SelectionVector &remaining_sel) {
	auto rhs_scalar = rhs_column.GetValue(rhs_row);
	if (rhs_scalar.type() != rhs_value.GetType()) {
		rhs_scalar = rhs_scalar.DefaultCastAs(rhs_value.GetType());
	}
	if (rhs_scalar.IsNull()) {
		ConstantVector::SetNull(rhs_value, count_t(candidate_count));
	} else {
		ConstantVector::Reference(rhs_value, rhs_scalar, count_t(candidate_count));
	}
	Vector lhs_slice(lhs_column, candidate_sel, candidate_count);

	state.null_mask.SetAllValid(candidate_count);
	const idx_t true_count = SelectComparison(comparison_type, lhs_slice, rhs_value, nullptr, candidate_count,
	                                          &state.equal_sel, &state.unequal_sel, &state.null_mask);

	idx_t remaining_count = 0;
	for (idx_t i = 0; i < true_count; i++) {
		auto local_idx = state.equal_sel.get_index(i);
		remaining_sel.set_index(remaining_count++, candidate_sel.get_index(local_idx));
	}
	for (idx_t i = 0; i < candidate_count - true_count; i++) {
		const auto local_idx = state.unequal_sel.get_index(i);
		if (!state.null_mask.RowIsValid(local_idx)) {
			const auto row_idx = candidate_sel.get_index(local_idx);
			remaining_sel.set_index(remaining_count++, row_idx);
			state.matched_mask.SetValid(row_idx);
		}
	}
	return remaining_count;
}

idx_t MatchConditionScanRow(DataChunk &join_keys, DataChunk &scan_chunk, idx_t scan_row,
                            const SelectionVector &unresolved_sel, idx_t unresolved_count,
                            const vector<JoinCondition> &conditions, MarkJoinNullMatchState &state) {
	reference<const SelectionVector> candidate_sel(unresolved_sel);
	idx_t candidate_count = unresolved_count;
	bool use_a = true;

	state.ResetMatchedMask();
	for (idx_t cond_idx = 0; cond_idx < conditions.size(); cond_idx++) {
		auto &remaining_sel = use_a ? state.candidate_sel_a : state.candidate_sel_b;
		candidate_count = FilterCandidatesForConditionComparison(
		    join_keys.data[cond_idx], scan_chunk.data[cond_idx], state.rhs_constant_values[cond_idx], scan_row,
		    scan_chunk.size(), candidate_sel.get(), candidate_count, conditions[cond_idx].GetComparisonType(), state,
		    remaining_sel);
		candidate_sel = remaining_sel;
		use_a = !use_a;
		if (candidate_count == 0) {
			break;
		}
	}

	idx_t matched_count = 0;
	for (idx_t i = 0; i < candidate_count; i++) {
		const auto row_idx = candidate_sel.get().get_index(i);
		if (state.matched_mask.RowIsValid(row_idx)) {
			state.row_match_sel.set_index(matched_count++, row_idx);
		}
	}
	return matched_count;
}

} // namespace

void MarkJoinPostProcessor::Initialize(ClientContext &context_p, BufferManager &buffer_manager_p, JoinType join_type_p,
                                       idx_t condition_count_p, const vector<ExpressionType> &equality_predicates_p,
                                       const vector<LogicalType> &condition_types_p) {
	context = context_p;
	buffer_manager = buffer_manager_p;
	join_type = join_type_p;
	condition_count = condition_count_p;
	equality_predicates = equality_predicates_p;
	condition_types = condition_types_p;
	state.strategy = ChooseStrategy();
	state.null_remainder.enabled = UsesNullRemainder();
}

MarkNullStrategy MarkJoinPostProcessor::ChooseStrategy() const {
	if (join_type != JoinType::MARK) {
		return MarkNullStrategy::NONE;
	}
	bool has_null_sensitive_condition = false;
	for (auto predicate : equality_predicates) {
		if (ComparisonPropagatesNull(predicate)) {
			has_null_sensitive_condition = true;
			break;
		}
	}
	if (!has_null_sensitive_condition) {
		return MarkNullStrategy::NONE;
	}
	for (idx_t i = 0; i < equality_predicates.size() && i < condition_types.size(); i++) {
		if (equality_predicates[i] == ExpressionType::COMPARE_NOTEQUAL && IsUnnamedStructType(condition_types[i])) {
			return MarkNullStrategy::FULL_SCAN;
		}
	}
	if (condition_count <= 1 || equality_predicates.size() != condition_count) {
		return MarkNullStrategy::SIMPLE_HAS_NULL;
	}
	for (auto predicate : equality_predicates) {
		if (predicate != ExpressionType::COMPARE_EQUAL) {
			return MarkNullStrategy::FULL_SCAN;
		}
	}
	return MarkNullStrategy::NULL_REMAINDER;
}

bool MarkJoinPostProcessor::UsesCorrelatedCounts() const {
	return state.strategy == MarkNullStrategy::CORRELATED_COUNTS;
}

bool MarkJoinPostProcessor::UsesNullRemainder() const {
	return state.strategy == MarkNullStrategy::NULL_REMAINDER;
}

bool MarkJoinPostProcessor::UsesConditionScan() const {
	return state.strategy == MarkNullStrategy::FULL_SCAN;
}

void MarkJoinPostProcessor::InitializeCorrelatedCounts(const vector<LogicalType> &correlated_types) {
	D_ASSERT(join_type == JoinType::MARK);
	D_ASSERT(context);
	auto &info = state.correlated_counts;
	const auto last_key_idx = correlated_types.size();
	info.payload_masks_nested_null = last_key_idx < equality_predicates.size() &&
	                                 last_key_idx < condition_types.size() &&
	                                 equality_predicates[last_key_idx] == ExpressionType::COMPARE_NOTEQUAL &&
	                                 IsUnnamedStructType(condition_types[last_key_idx]);

	vector<LogicalType> delim_payload_types;
	vector<BoundAggregateExpression *> correlated_aggregates;
	unique_ptr<BoundAggregateExpression> aggr;

	FunctionBinder function_binder(*context);
	aggr = function_binder.BindAggregateFunction(CountStarFun::GetFunction(), {}, nullptr, AggregateType::NON_DISTINCT);
	correlated_aggregates.push_back(&*aggr);
	delim_payload_types.push_back(aggr->GetReturnType());
	info.correlated_aggregates.push_back(std::move(aggr));

	auto count_fun = CountFunctionBase::GetFunction();
	vector<unique_ptr<Expression>> children;
	children.push_back(make_uniq_base<Expression, BoundReferenceExpression>(
	    info.payload_masks_nested_null ? LogicalType::BOOLEAN : condition_types[last_key_idx], 0U));
	aggr = function_binder.BindAggregateFunction(count_fun, std::move(children), nullptr, AggregateType::NON_DISTINCT);
	correlated_aggregates.push_back(&*aggr);
	delim_payload_types.push_back(aggr->GetReturnType());
	info.correlated_aggregates.push_back(std::move(aggr));

	auto &allocator = BufferAllocator::Get(*context);
	info.correlated_counts = make_uniq<GroupedAggregateHashTable>(*context, allocator, correlated_types,
	                                                              delim_payload_types, correlated_aggregates);
	info.correlated_types = correlated_types;
	info.group_chunk.Initialize(allocator, correlated_types);
	info.result_chunk.Initialize(allocator, delim_payload_types);
	state.strategy = MarkNullStrategy::CORRELATED_COUNTS;
}

void MarkJoinPostProcessor::SinkBuildKeys(DataChunk &keys) {
	if (UsesCorrelatedCounts()) {
		auto &info = state.correlated_counts;
		lock_guard<mutex> mj_lock(info.lock);
		D_ASSERT(info.correlated_counts);
		for (idx_t i = 0; i < info.correlated_types.size(); i++) {
			info.group_chunk.data[i].Reference(keys.data[i]);
		}
		info.group_chunk.CheckCardinality(keys.size());
		if (info.correlated_payload.data.empty()) {
			vector<LogicalType> types;
			types.push_back(info.payload_masks_nested_null ? LogicalType::BOOLEAN
			                                               : keys.data[info.correlated_types.size()].GetType());
			info.correlated_payload.InitializeEmpty(types);
		}
		const auto last_key_idx = info.correlated_types.size();
		if (info.payload_masks_nested_null) {
			auto &payload = info.correlated_payload.data[0];
			payload.Reference(Value::BOOLEAN(true), count_t(keys.size()));
			payload.Flatten();
			auto payload_data = FlatVector::GetDataMutable<bool>(payload);
			auto &validity = FlatVector::ValidityMutable(payload);
			validity.SetAllValid(keys.size());
			for (idx_t row_idx = 0; row_idx < keys.size(); row_idx++) {
				payload_data[row_idx] = true;
				if (MarkJoinValueContainsNestedNull(keys.data[last_key_idx].GetValue(row_idx))) {
					validity.SetInvalid(row_idx);
				}
			}
		} else {
			info.correlated_payload.data[0].Reference(keys.data[last_key_idx]);
		}
		info.correlated_payload.CheckCardinality(keys.size());
		info.correlated_counts->AddChunk(info.group_chunk, info.correlated_payload, AggregateType::NON_DISTINCT);
	}
	RegisterNullRemainderRows(keys);
}

void MarkJoinPostProcessor::RegisterNullRemainderRows(DataChunk &keys) {
	if (!UsesNullRemainder()) {
		return;
	}
	vector<VectorValidityIterator> validities;
	validities.reserve(keys.ColumnCount());
	bool can_have_null = false;
	for (idx_t col_idx = 0; col_idx < keys.ColumnCount(); col_idx++) {
		validities.emplace_back(keys.data[col_idx]);
		can_have_null = can_have_null || validities.back().CanHaveNull();
	}
	if (!can_have_null) {
		return;
	}
	ValidityMask has_any_null(keys.size());
	has_any_null.SetAllInvalid(keys.size());
	ValidityMask has_all_null(keys.size());
	has_all_null.Initialize();

	for (auto &validity : validities) {
		if (!validity.CanHaveNull()) {
			has_all_null.SetAllInvalid(keys.size());
			continue;
		}
		for (idx_t row_idx = 0; row_idx < keys.size(); row_idx++) {
			if (validity.IsValid(row_idx)) {
				has_all_null.SetInvalidUnsafe(row_idx);
			} else {
				has_any_null.SetValidUnsafe(row_idx);
			}
		}
	}
	auto &null_info = state.null_remainder;
	null_info.enabled = true;
	InitializeMarkJoinNullRemainder(null_info.remainder, *buffer_manager, condition_types);
	SelectionVector null_sel(STANDARD_VECTOR_SIZE);

	idx_t null_count = 0;
	for (idx_t row_idx = 0; row_idx < keys.size(); row_idx++) {
		null_sel.set_index(null_count, row_idx);
		null_count += has_any_null.RowIsValidUnsafe(row_idx);
	}
	null_info.has_null_rows |= null_count > 0;
	null_info.has_all_null |= has_all_null.CountValid(keys.size()) > 0;

	if (null_count > 0) {
		null_info.remainder.data->Append(null_info.remainder.append_state, keys, null_sel, null_count);
	}
}

void MarkJoinPostProcessor::Merge(MarkJoinPostProcessor &other, bool &has_null) {
	if (join_type == JoinType::MARK && UsesCorrelatedCounts()) {
		auto &info = state.correlated_counts;
		lock_guard<mutex> mj_lock(info.lock);
		auto &other_info = other.state.correlated_counts;
		info.correlated_counts->Combine(*other_info.correlated_counts);
	}
	MergeNullRemainderRows(other, has_null);
}

void MarkJoinPostProcessor::Reset() {
	if (UsesCorrelatedCounts()) {
		auto &info = state.correlated_counts;
		vector<BoundAggregateExpression *> correlated_aggregates;
		vector<LogicalType> payload_types;
		correlated_aggregates.reserve(info.correlated_aggregates.size());
		payload_types.reserve(info.correlated_aggregates.size());
		for (auto &expr : info.correlated_aggregates) {
			auto &aggr = expr->Cast<BoundAggregateExpression>();
			correlated_aggregates.push_back(&aggr);
			payload_types.push_back(aggr.GetReturnType());
		}
		auto &allocator = BufferAllocator::Get(*context);
		info.correlated_counts = make_uniq<GroupedAggregateHashTable>(*context, allocator, info.correlated_types,
		                                                              payload_types, correlated_aggregates);
		info.group_chunk.Reset();
		info.correlated_payload.Reset();
		info.result_chunk.Reset();
	}
	if (state.null_remainder.enabled) {
		auto &null_info = state.null_remainder;
		null_info.has_null_rows = false;
		null_info.has_all_null = false;
		if (null_info.remainder.layout) {
			null_info.remainder.data =
			    make_uniq<TupleDataCollection>(*buffer_manager, null_info.remainder.layout, MemoryTag::HASH_TABLE);
			null_info.remainder.data->InitializeAppend(null_info.remainder.append_state);
		}
	}
}

void MarkJoinPostProcessor::MergeNullRemainderRows(MarkJoinPostProcessor &other, bool &has_null) {
	if (!UsesNullRemainder() || !other.state.null_remainder.enabled) {
		return;
	}
	auto &null_info = state.null_remainder;
	auto &other_null_info = other.state.null_remainder;
	lock_guard<mutex> guard(null_info.lock);
	has_null = has_null || other_null_info.has_null_rows;
	null_info.has_null_rows = null_info.has_null_rows || other_null_info.has_null_rows;
	null_info.has_all_null = null_info.has_all_null || other_null_info.has_all_null;
	if (!other_null_info.remainder.data) {
		return;
	}
	InitializeMarkJoinNullRemainder(null_info.remainder, *buffer_manager, condition_types);
	null_info.remainder.data->Combine(*other_null_info.remainder.data);
}

void MarkJoinPostProcessor::ApplyJoinKeyNullMask(DataChunk &join_keys, const vector<bool> &null_values_are_equal,
                                                 ValidityMask &mask) const {
	for (idx_t col_idx = 0; col_idx < join_keys.ColumnCount(); col_idx++) {
		if (null_values_are_equal[col_idx]) {
			continue;
		}
		UnifiedVectorFormat jdata;
		join_keys.data[col_idx].ToUnifiedFormat(jdata);
		if (jdata.validity.CanHaveNull()) {
			for (idx_t i = 0; i < join_keys.size(); i++) {
				auto jidx = jdata.sel->get_index(i);
				if (!jdata.validity.RowIsValidUnsafe(jidx)) {
					mask.SetInvalid(i);
				}
			}
		}
	}
}

void MarkJoinPostProcessor::InitializeMarkJoinResult(DataChunk &join_keys, DataChunk &probe_data, DataChunk &result,
                                                     const vector<bool> &null_values_are_equal, bool *&bool_result,
                                                     ValidityMask *&mask,
                                                     optional_ptr<const vector<idx_t>> lhs_output_columns) const {
	result.SetChildCardinality(probe_data.size());
	if (lhs_output_columns) {
		for (idx_t i = 0; i < lhs_output_columns->size(); i++) {
			result.data[i].Reference(probe_data.data[(*lhs_output_columns)[i]]);
		}
	} else {
		for (idx_t i = 0; i < probe_data.ColumnCount(); i++) {
			result.data[i].Reference(probe_data.data[i]);
		}
	}

	auto &result_vector = result.data.back();
	result_vector.SetVectorType(VectorType::FLAT_VECTOR);
	FlatVector::SetSize(result_vector, count_t(probe_data.size()));
	bool_result = FlatVector::GetDataMutable<bool>(result_vector);
	mask = &FlatVector::ValidityMutable(result_vector);
	for (idx_t i = 0; i < probe_data.size(); i++) {
		bool_result[i] = false;
	}
	ApplyJoinKeyNullMask(join_keys, null_values_are_equal, *mask);
}

void MarkJoinPostProcessor::ConstructResult(DataChunk &join_keys, DataChunk &probe_data, DataChunk &result,
                                            const vector<idx_t> &lhs_output_columns,
                                            const vector<bool> &null_values_are_equal, const bool *found_match,
                                            bool has_null) {
	bool *bool_result;
	ValidityMask *mask;
	InitializeMarkJoinResult(join_keys, probe_data, result, null_values_are_equal, bool_result, mask,
	                         lhs_output_columns);
	D_ASSERT(found_match);
	for (idx_t i = 0; i < probe_data.size(); i++) {
		bool_result[i] = found_match[i];
	}
	RefineUnmatchedRows(join_keys, *mask, bool_result, has_null);
}

void MarkJoinPostProcessor::ConstructEmptyResult(DataChunk &join_keys, DataChunk &probe_data, DataChunk &result,
                                                 const vector<idx_t> &lhs_output_columns,
                                                 const vector<bool> &null_values_are_equal, bool has_null) {
	bool *bool_result;
	ValidityMask *mask;
	InitializeMarkJoinResult(join_keys, probe_data, result, null_values_are_equal, bool_result, mask,
	                         lhs_output_columns);
	RefineUnmatchedRows(join_keys, *mask, bool_result, has_null);
}

void MarkJoinPostProcessor::ConstructResult(DataChunk &join_keys, DataChunk &probe_data, DataChunk &result,
                                            const vector<bool> &null_values_are_equal, const bool *found_match,
                                            ColumnDataCollection &rhs_condition_data,
                                            const vector<JoinCondition> &conditions, bool has_null) {
	bool *bool_result;
	ValidityMask *mask;
	InitializeMarkJoinResult(join_keys, probe_data, result, null_values_are_equal, bool_result, mask);
	D_ASSERT(found_match);
	for (idx_t i = 0; i < probe_data.size(); i++) {
		bool_result[i] = found_match[i];
	}
	ProbeConditionScanRows(join_keys, *mask, bool_result, rhs_condition_data, conditions, has_null);
}

void MarkJoinPostProcessor::RefineUnmatchedRows(DataChunk &join_keys, ValidityMask &mask, const bool *found_match,
                                                bool has_null) {
	if (UsesNullRemainder()) {
		ProbeNullRemainderRows(join_keys, mask, found_match);
	} else if (has_null) {
		for (idx_t i = 0; i < join_keys.size(); i++) {
			if (!found_match[i]) {
				mask.SetInvalid(i);
			}
		}
	}
}

void MarkJoinPostProcessor::ProbeConditionScanRows(DataChunk &join_keys, ValidityMask &mask, const bool *found_match,
                                                   ColumnDataCollection &rhs_condition_data,
                                                   const vector<JoinCondition> &conditions, bool has_null) {
	D_ASSERT(UsesConditionScan());

	SelectionVector unresolved_sel(STANDARD_VECTOR_SIZE);
	idx_t unresolved_count = BuildUnresolvedSelection(found_match, mask, join_keys.size(), unresolved_sel);
	if (unresolved_count == 0) {
		return;
	}

	ColumnDataScanState scan_state;
	rhs_condition_data.InitializeScan(scan_state);
	DataChunk scan_chunk;
	rhs_condition_data.InitializeScanChunk(scan_chunk);
	SelectionVector matched_sel(STANDARD_VECTOR_SIZE);
	SelectionVector remaining_sel(STANDARD_VECTOR_SIZE);
	MarkJoinNullMatchState match_state(join_keys.size(), condition_types);

	while (unresolved_count > 0 && rhs_condition_data.Scan(scan_state, scan_chunk)) {
		for (idx_t scan_row = 0; scan_row < scan_chunk.size() && unresolved_count > 0; scan_row++) {
			const idx_t matched_count = MatchConditionScanRow(join_keys, scan_chunk, scan_row, unresolved_sel,
			                                                  unresolved_count, conditions, match_state);
			if (matched_count == 0) {
				continue;
			}
			InvalidateSelection(mask, match_state.row_match_sel, matched_count);
			unresolved_count = CompactUnresolvedSelection(unresolved_sel, unresolved_count, match_state.row_match_sel,
			                                              matched_count, remaining_sel);
			for (idx_t i = 0; i < unresolved_count; i++) {
				unresolved_sel.set_index(i, remaining_sel.get_index(i));
			}
		}
	}
}

void MarkJoinPostProcessor::ProbeNullRemainderRows(DataChunk &join_keys, ValidityMask &mask, const bool *found_match) {
	D_ASSERT(UsesNullRemainder());
	auto &null_info = state.null_remainder;
	if (!null_info.has_null_rows) {
		return;
	}
	SelectionVector unresolved_sel(STANDARD_VECTOR_SIZE);
	idx_t unresolved_count = BuildUnresolvedSelection(found_match, mask, join_keys.size(), unresolved_sel);
	if (unresolved_count == 0) {
		return;
	}
	if (null_info.has_all_null) {
		InvalidateSelection(mask, unresolved_sel, unresolved_count);
		return;
	}
	if (!null_info.remainder.data) {
		return;
	}

	TupleDataScanState scan_state;
	null_info.remainder.data->InitializeScan(scan_state);
	DataChunk scan_chunk;
	null_info.remainder.data->InitializeScanChunk(scan_state, scan_chunk);
	SelectionVector matched_sel(STANDARD_VECTOR_SIZE);
	SelectionVector remaining_sel(STANDARD_VECTOR_SIZE);
	MarkJoinNullMatchState match_state(join_keys.size(), null_info.remainder.key_types);

	while (unresolved_count > 0 && null_info.remainder.data->Scan(scan_state, scan_chunk)) {
		const idx_t matched_count =
		    MatchNullRemainderChunk(join_keys, unresolved_sel, unresolved_count, scan_chunk, matched_sel, match_state);
		if (matched_count == 0) {
			continue;
		}
		InvalidateSelection(mask, matched_sel, matched_count);
		unresolved_count =
		    CompactUnresolvedSelection(unresolved_sel, unresolved_count, matched_sel, matched_count, remaining_sel);
		for (idx_t i = 0; i < unresolved_count; i++) {
			unresolved_sel.set_index(i, remaining_sel.get_index(i));
		}
	}
}

void MarkJoinPostProcessor::ConstructCorrelatedMarkResult(DataChunk &keys, DataChunk &probe_data, DataChunk &result,
                                                          const vector<idx_t> &lhs_output_in_probe,
                                                          const bool *found_match) {
	D_ASSERT(UsesCorrelatedCounts());
	auto &info = state.correlated_counts;
	lock_guard<mutex> mj_lock(info.lock);

	D_ASSERT(keys.ColumnCount() == info.group_chunk.ColumnCount() + 1);
	for (idx_t i = 0; i < info.group_chunk.ColumnCount(); i++) {
		info.group_chunk.data[i].Reference(keys.data[i]);
	}
	info.group_chunk.CheckCardinality(keys.size());
	info.correlated_counts->FetchAggregates(info.group_chunk, info.result_chunk);

	result.SetChildCardinality(probe_data.size());
	for (idx_t i = 0; i < lhs_output_in_probe.size(); i++) {
		idx_t probe_col_idx = lhs_output_in_probe[i];
		result.data[i].Reference(probe_data.data[probe_col_idx]);
	}

	auto &last_key = keys.data.back();
	auto &result_vector = result.data.back();
	result_vector.SetVectorType(VectorType::FLAT_VECTOR);
	auto bool_result = FlatVector::GetDataMutable<bool>(result_vector);
	auto &mask = FlatVector::ValidityMutable(result_vector);

	switch (last_key.GetVectorType()) {
	case VectorType::CONSTANT_VECTOR:
		if (ConstantVector::IsNull(last_key)) {
			mask.SetAllInvalid(probe_data.size());
		}
		break;
	case VectorType::FLAT_VECTOR:
		mask.Copy(FlatVector::ValidityMutable(last_key), probe_data.size());
		break;
	default: {
		UnifiedVectorFormat kdata;
		last_key.ToUnifiedFormat(kdata);
		for (idx_t i = 0; i < probe_data.size(); i++) {
			auto kidx = kdata.sel->get_index(i);
			mask.Set(i, kdata.validity.RowIsValid(kidx));
		}
		break;
	}
	}

	auto count_star = FlatVector::GetData<int64_t>(info.result_chunk.data[0]);
	auto count = FlatVector::GetData<int64_t>(info.result_chunk.data[1]);

	for (idx_t i = 0; i < probe_data.size(); i++) {
		D_ASSERT(count_star[i] >= count[i]);
		bool_result[i] = found_match ? found_match[i] : false;
		if (!bool_result[i] && count_star[i] > count[i]) {
			mask.SetInvalid(i);
		}
		if (count_star[i] == 0) {
			mask.SetValid(i);
		}
	}
	FlatVector::SetSize(result_vector, count_t(probe_data.size()));
}

} // namespace duckdb
