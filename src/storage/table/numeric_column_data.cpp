#include "duckdb/storage/table/numeric_column_data.hpp"

#include "duckdb/common/exception/transaction_exception.hpp"

#include "duckdb/common/types/numeric_type.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/function/scalar/generic_functions.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/storage/statistics/unbounded_numeric_stats.hpp"
#include "duckdb/storage/table/append_state.hpp"
#include "duckdb/storage/table/column_checkpoint_state.hpp"
#include "duckdb/storage/table/scan_state.hpp"

namespace duckdb {

NumericColumnData::NumericColumnData(BlockManager &block_manager, DataTableInfo &info, idx_t column_index,
                                     LogicalType type_p, ColumnDataType data_type, optional_ptr<ColumnData> parent)
    : ColumnData(block_manager, info, column_index, std::move(type_p), data_type, parent) {
	D_ASSERT(type.id() == LogicalTypeId::NUMERIC);
	if (data_type != ColumnDataType::CHECKPOINT_TARGET) {
		// an empty column is stored in the default representation - the first append widens it when needed
		inner = CreateInner(NumericType::DefaultType());
	}
}

shared_ptr<ColumnData> NumericColumnData::CreateInner(const LogicalType &storage_type) {
	return ColumnData::CreateColumn(block_manager, info, column_index, storage_type, GetDataType(), this);
}

shared_ptr<ColumnData> NumericColumnData::GetInner() const {
	lock_guard<mutex> guard(inner_lock);
	return inner;
}

LogicalType NumericColumnData::GetStorageType() const {
	return GetInner()->type;
}

void NumericColumnData::SetInner(shared_ptr<ColumnData> inner_p) {
	lock_guard<mutex> guard(inner_lock);
	inner = std::move(inner_p);
	inner->SetParent(*this);
}

ColumnData &NumericColumnData::Pinned(ColumnScanState &state) {
	if (!state.pinned_column) {
		state.pinned_column = GetInner();
	}
	return *state.pinned_column;
}

void NumericColumnData::CastVector(const Vector &source, Vector &target, idx_t count) {
	if (source.GetType() == target.GetType()) {
		target.Reference(source);
		return;
	}
	Vector source_ref(source.GetType());
	source_ref.Reference(source);
	// the representations are chosen so that every value fits - a failure here is a bug
	VectorOperations::DefaultCast(source_ref, target, count, true);
}

//----------------------------------------------------------------------------------------------------------------------
// Scan
//----------------------------------------------------------------------------------------------------------------------
idx_t NumericColumnData::GetMaxEntry() {
	return GetInner()->GetMaxEntry();
}

void NumericColumnData::InitializePrefetch(PrefetchState &prefetch_state, ColumnScanState &scan_state, idx_t rows) {
	Pinned(scan_state).InitializePrefetch(prefetch_state, scan_state, rows);
}

void NumericColumnData::InitializeScan(ColumnScanState &state) {
	state.pinned_column = GetInner();
	// the scan state was initialized for the NUMERIC type - re-initialize it for the storage type of this row group
	state.child_states.clear();
	state.Initialize(state.context, state.pinned_column->type, state.scan_options);
	state.pinned_column->InitializeScan(state);
}

void NumericColumnData::InitializeScanWithOffset(ColumnScanState &state, idx_t row_idx) {
	state.pinned_column = GetInner();
	state.child_states.clear();
	state.Initialize(state.context, state.pinned_column->type, state.scan_options);
	state.pinned_column->InitializeScanWithOffset(state, row_idx);
}

idx_t NumericColumnData::Scan(TransactionData transaction, idx_t vector_index, ColumnScanState &state, Vector &result,
                              idx_t target_count) {
	auto &column = Pinned(state);
	if (column.type == result.GetType()) {
		return column.Scan(transaction, vector_index, state, result, target_count);
	}
	Vector intermediate(column.type, target_count);
	auto scan_count = column.Scan(transaction, vector_index, state, intermediate, target_count);
	CastVector(intermediate, result, scan_count);
	return scan_count;
}

idx_t NumericColumnData::ScanCount(ColumnScanState &state, Vector &result, idx_t target_count, idx_t result_offset) {
	auto &column = Pinned(state);
	if (column.type == result.GetType()) {
		return column.ScanCount(state, result, target_count, result_offset);
	}
	Vector intermediate(column.type, target_count);
	auto scan_count = column.ScanCount(state, intermediate, target_count, 0);
	Vector cast_vector(result.GetType(), scan_count);
	CastVector(intermediate, cast_vector, scan_count);
	VectorOperations::Copy(cast_vector, result, scan_count, 0, result_offset);
	return scan_count;
}

void NumericColumnData::Skip(ColumnScanState &state, idx_t count) {
	Pinned(state).Skip(state, count);
}

//----------------------------------------------------------------------------------------------------------------------
// Zonemaps
//----------------------------------------------------------------------------------------------------------------------
static bool FindReferenceType(const Expression &expr, LogicalType &result) {
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_REF) {
		result = expr.GetReturnType();
		return true;
	}
	bool found = false;
	ExpressionIterator::EnumerateChildren(expr, [&](const Expression &child) {
		if (!found) {
			found = FindReferenceType(child, result);
		}
	});
	return found;
}

LogicalType NumericColumnData::FilterColumnType(const TableFilter &filter, const LogicalType &fallback) {
	auto &expr_filter = ExpressionFilter::GetExpressionFilter(filter, "NumericColumnData::FilterColumnType");
	LogicalType result;
	if (FindReferenceType(*expr_filter.expr, result)) {
		return result;
	}
	return fallback;
}

FilterPropagateResult NumericColumnData::CheckZonemap(ColumnScanState &state, TableFilter &filter,
                                                      optional_ptr<SegmentNode<ColumnSegment>> &checked_segment) {
	checked_segment = nullptr;
	if (state.segment_checked) {
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}
	if (!state.current) {
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}
	auto &column = Pinned(state);
	auto &expr_filter = ExpressionFilter::GetExpressionFilter(filter, "NumericColumnData::CheckZonemap");
	bool is_dynamic = ExpressionFilter::ContainsInternalFunction(*expr_filter.expr, DynamicFilterScalarFun::NAME);
	state.segment_checked = !is_dynamic;
	auto context = state.context.GetClientContext();
	if (IsDirectNullCheckFilter(filter) && !state.child_states.empty() && state.child_states[0].current) {
		// NULL checks are judged on the validity segment, whose statistics do not depend on the representation
		checked_segment = state.child_states[0].current;
		auto validity_stats = column.GetSegmentStatistics(*checked_segment);
		return context ? expr_filter.CheckStatistics(*context, *validity_stats)
		               : expr_filter.CheckStatistics(*validity_stats);
	}
	checked_segment = state.current;
	// the segment holds statistics of the storage type - the filter compares against the bound type
	auto filter_type = FilterColumnType(filter, column.type);
	auto segment_stats = column.GetSegmentStatistics(*checked_segment);
	auto typed_stats = UnboundedNumericStats::ToTyped(UnboundedNumericStats::FromTyped(*segment_stats), filter_type);
	auto prune_result =
	    context ? expr_filter.CheckStatistics(*context, typed_stats) : expr_filter.CheckStatistics(typed_stats);
	if (prune_result == FilterPropagateResult::NO_PRUNING_POSSIBLE) {
		return prune_result;
	}
	auto update_stats = column.GetUpdateStatistics();
	if (!update_stats) {
		return prune_result;
	}
	auto typed_update_stats =
	    UnboundedNumericStats::ToTyped(UnboundedNumericStats::FromTyped(*update_stats), filter_type);
	auto update_result = context ? expr_filter.CheckStatistics(*context, typed_update_stats)
	                             : expr_filter.CheckStatistics(typed_update_stats);
	if (prune_result == update_result) {
		return prune_result;
	}
	return FilterPropagateResult::NO_PRUNING_POSSIBLE;
}

FilterPropagateResult NumericColumnData::CheckZonemap(optional_ptr<ClientContext> context, const StorageIndex &index,
                                                      TableFilter &filter) {
	auto &expr_filter = ExpressionFilter::GetExpressionFilter(filter, "NumericColumnData::CheckZonemap");
	unique_lock<mutex> l;
	if (stats) {
		l = unique_lock<mutex>(stats_lock);
	}
	// a nested NUMERIC column keeps its statistics in its parent
	auto &column_stats = GetStatisticsRef();
	auto filter_type = FilterColumnType(filter, UnboundedNumericStats::ResolveType(column_stats));
	auto typed_stats = UnboundedNumericStats::ToTyped(column_stats, filter_type);
	return context ? expr_filter.CheckStatistics(*context, typed_stats) : expr_filter.CheckStatistics(typed_stats);
}

//----------------------------------------------------------------------------------------------------------------------
// Append
//----------------------------------------------------------------------------------------------------------------------
template <class T>
static bool FindMinMax(UnifiedVectorFormat &vdata, idx_t count, T &min, T &max, bool &has_null) {
	auto data = UnifiedVectorFormat::GetData<T>(vdata);
	bool found = false;
	for (idx_t i = 0; i < count; i++) {
		auto idx = vdata.sel->get_index(i);
		if (!vdata.validity.RowIsValid(idx)) {
			has_null = true;
			continue;
		}
		if (!found) {
			min = data[idx];
			max = data[idx];
			found = true;
			continue;
		}
		if (data[idx] < min) {
			min = data[idx];
		}
		if (data[idx] > max) {
			max = data[idx];
		}
	}
	return found;
}

//! A value of an exact representation from its physical value
static Value ExactValue(const LogicalType &type, int8_t value) {
	if (type.id() == LogicalTypeId::DECIMAL) {
		return Value::DECIMAL(int16_t(value), DecimalType::GetWidth(type), DecimalType::GetScale(type));
	}
	return Value::Numeric(type, int64_t(value));
}
static Value ExactValue(const LogicalType &type, int16_t value) {
	if (type.id() == LogicalTypeId::DECIMAL) {
		return Value::DECIMAL(value, DecimalType::GetWidth(type), DecimalType::GetScale(type));
	}
	return Value::Numeric(type, int64_t(value));
}
static Value ExactValue(const LogicalType &type, int32_t value) {
	if (type.id() == LogicalTypeId::DECIMAL) {
		return Value::DECIMAL(value, DecimalType::GetWidth(type), DecimalType::GetScale(type));
	}
	return Value::Numeric(type, int64_t(value));
}
static Value ExactValue(const LogicalType &type, int64_t value) {
	if (type.id() == LogicalTypeId::DECIMAL) {
		return Value::DECIMAL(value, DecimalType::GetWidth(type), DecimalType::GetScale(type));
	}
	return Value::Numeric(type, value);
}
static Value ExactValue(const LogicalType &type, hugeint_t value) {
	if (type.id() == LogicalTypeId::DECIMAL) {
		return Value::DECIMAL(value, DecimalType::GetWidth(type), DecimalType::GetScale(type));
	}
	return Value::Numeric(type, value);
}

//! The number of fractional digits the values of a decimal vector need: trailing zeros do not count, so whole numbers
//! stored as decimals keep the column in an integer representation
template <class T>
static uint8_t NeededScale(UnifiedVectorFormat &vdata, idx_t count, uint8_t scale) {
	auto data = UnifiedVectorFormat::GetData<T>(vdata);
	uint8_t needed = 0;
	for (idx_t i = 0; i < count && needed < scale; i++) {
		auto idx = vdata.sel->get_index(i);
		if (!vdata.validity.RowIsValid(idx)) {
			continue;
		}
		auto value = data[idx];
		uint8_t trailing_zeros = 0;
		while (trailing_zeros < scale && value % 10 == 0) {
			value /= 10;
			trailing_zeros++;
		}
		needed = MaxValue<uint8_t>(needed, scale - trailing_zeros);
	}
	return needed;
}

//! Widen the unbounded statistics with the values of a vector of an exact representation
static void UpdateUnboundedStats(BaseStatistics &stats, const Vector &vector, idx_t count) {
	UnifiedVectorFormat vdata;
	vector.ToUnifiedFormat(count, vdata);
	auto &type = vector.GetType();
	bool has_null = false;
	bool found = false;
	Value min, max;
	uint8_t type_scale = type.id() == LogicalTypeId::DECIMAL ? DecimalType::GetScale(type) : 0;
	uint8_t needed_scale = type_scale;
	switch (type.InternalType()) {
	case PhysicalType::INT8: {
		int8_t lo, hi;
		found = FindMinMax<int8_t>(vdata, count, lo, hi, has_null);
		if (found) {
			min = ExactValue(type, lo);
			max = ExactValue(type, hi);
		}
		break;
	}
	case PhysicalType::INT16: {
		int16_t lo, hi;
		found = FindMinMax<int16_t>(vdata, count, lo, hi, has_null);
		if (found) {
			min = ExactValue(type, lo);
			max = ExactValue(type, hi);
			if (type_scale > 0) {
				needed_scale = NeededScale<int16_t>(vdata, count, type_scale);
			}
		}
		break;
	}
	case PhysicalType::INT32: {
		int32_t lo, hi;
		found = FindMinMax<int32_t>(vdata, count, lo, hi, has_null);
		if (found) {
			min = ExactValue(type, lo);
			max = ExactValue(type, hi);
			if (type_scale > 0) {
				needed_scale = NeededScale<int32_t>(vdata, count, type_scale);
			}
		}
		break;
	}
	case PhysicalType::INT64: {
		int64_t lo, hi;
		found = FindMinMax<int64_t>(vdata, count, lo, hi, has_null);
		if (found) {
			min = ExactValue(type, lo);
			max = ExactValue(type, hi);
			if (type_scale > 0) {
				needed_scale = NeededScale<int64_t>(vdata, count, type_scale);
			}
		}
		break;
	}
	case PhysicalType::INT128: {
		hugeint_t lo, hi;
		found = FindMinMax<hugeint_t>(vdata, count, lo, hi, has_null);
		if (found) {
			min = ExactValue(type, lo);
			max = ExactValue(type, hi);
			if (type_scale > 0) {
				needed_scale = NeededScale<hugeint_t>(vdata, count, type_scale);
			}
		}
		break;
	}
	case PhysicalType::VARCHAR: {
		// wide decimals: BIGNUM blobs, ordered like their values
		string_t lo, hi;
		found = FindMinMax<string_t>(vdata, count, lo, hi, has_null);
		if (found) {
			min = Value::WIDEDECIMAL(lo.GetString(), DecimalType::GetWidth(type), DecimalType::GetScale(type));
			max = Value::WIDEDECIMAL(hi.GetString(), DecimalType::GetWidth(type), DecimalType::GetScale(type));
		}
		break;
	}
	default:
		throw InternalException("NUMERIC column stored in unexpected representation %s", type.ToString());
	}
	if (has_null) {
		stats.SetHasNull();
	}
	if (found) {
		stats.SetHasNoNull();
		if (needed_scale < type_scale) {
			// the values are whole numbers (or need fewer fractional digits than their type): record them that way
			auto reduced = LogicalType::DECIMAL(DecimalType::GetWidth(type), needed_scale);
			min = min.DefaultCastAs(reduced);
			max = max.DefaultCastAs(reduced);
		}
		UnboundedNumericStats::Update(stats, min);
		UnboundedNumericStats::Update(stats, max);
	}
}

static BaseStatistics &WrapperStats(ColumnAppendState &state) {
	if (!state.wrapper_stats) {
		state.wrapper_stats = UnboundedNumericStats::CreateEmpty(LogicalType::NUMERIC).ToUnique();
	}
	return *state.wrapper_stats;
}

void NumericColumnData::InitializeAppend(ColumnAppendState &state) {
	// appends are serialized by the row group - the inner column only changes inside Append
	GetInner()->InitializeAppend(state);
}

shared_ptr<ColumnData> NumericColumnData::Rebuild(ColumnData &source, const LogicalType &new_type) {
	auto new_column = CreateInner(new_type);
	idx_t total_count = source.count.load();
	if (total_count == 0) {
		return new_column;
	}
	ColumnScanState scan_state(nullptr);
	scan_state.Initialize(QueryContext(), source.type, nullptr);
	// committed updates are folded into the copy; uncommitted ones would be lost, so they abort the rebuild
	scan_state.update_scan_type = UpdateScanType::DISALLOW_UPDATES;
	source.InitializeScan(scan_state);

	ColumnAppendState append_state;
	new_column->InitializeAppend(append_state);

	DataChunk scan_chunk;
	scan_chunk.Initialize(Allocator::DefaultAllocator(), {source.type}, STANDARD_VECTOR_SIZE);
	DataChunk append_chunk;
	append_chunk.Initialize(Allocator::DefaultAllocator(), {new_type}, STANDARD_VECTOR_SIZE);

	idx_t vector_index = 0;
	for (idx_t scanned = 0; scanned < total_count; scanned += STANDARD_VECTOR_SIZE) {
		auto to_scan = MinValue<idx_t>(total_count - scanned, STANDARD_VECTOR_SIZE);
		scan_chunk.Reset();
		try {
			source.Scan(TransactionData::Unversioned(), vector_index++, scan_state, scan_chunk.data[0], to_scan);
		} catch (TransactionException &) {
			throw TransactionException("Cannot change the storage type of a NUMERIC column with outstanding updates - "
			                           "retry after the updating transaction has finished");
		}
		append_chunk.Reset();
		CastVector(scan_chunk.data[0], append_chunk.data[0], to_scan);
		new_column->Append(append_state, append_chunk.data[0], to_scan);
	}
	// flush the segment statistics - the column statistics are maintained by the caller
	auto rebuild_stats = BaseStatistics::CreateEmpty(new_type);
	ColumnDataFinalizeAppendState finalize_state(rebuild_stats);
	new_column->FinalizeAppend(finalize_state, append_state);
	return new_column;
}

void NumericColumnData::Widen(ColumnAppendState &state, const LogicalType &new_type) {
	auto current = GetInner();
	auto new_inner = Rebuild(*current, new_type);
	{
		lock_guard<mutex> guard(inner_lock);
		retired.push_back(std::move(inner));
		inner = new_inner;
	}
	// continue the append in the new column - the typed append state is re-initialized from scratch, the exact
	// statistics of the rows appended so far live in the wrapper statistics
	state.current = nullptr;
	state.child_appends.clear();
	state.append_state.reset();
	state.append_stats.reset();
	state.full_append_stats.reset();
	new_inner->InitializeAppend(state);
}

void NumericColumnData::Append(ColumnAppendState &state, const Vector &vector, idx_t append_count) {
	auto current = GetInner();
	auto &source_type = vector.GetType();
	if (source_type.id() == LogicalTypeId::SQLNULL) {
		// untyped NULLs (e.g. from a NULL parameter) are stored in the current representation
		Vector null_vector(Value(current->type), count_t(append_count));
		current->Append(state, null_vector, append_count);
		WrapperStats(state).SetHasNull();
		count += append_count;
		return;
	}
	if (source_type.id() == LogicalTypeId::VARCHAR) {
		// text (e.g. read by COPY FROM): the values decide the representation; TextType validated the text, so the
		// cast is not strict (leading zeros are fine)
		Vector parsed(NumericType::TextType(vector, append_count), append_count);
		Vector source_ref(source_type);
		source_ref.Reference(vector);
		VectorOperations::DefaultCast(source_ref, parsed, append_count, false);
		Append(state, parsed, append_count);
		return;
	}
	auto target_type = NumericType::Widen(current->type, source_type);
	if (target_type != current->type) {
		Widen(state, target_type);
		current = GetInner();
	}
	if (source_type == current->type) {
		current->Append(state, vector, append_count);
		UpdateUnboundedStats(WrapperStats(state), vector, append_count);
	} else {
		Vector cast_vector(current->type, append_count);
		CastVector(vector, cast_vector, append_count);
		current->Append(state, cast_vector, append_count);
		UpdateUnboundedStats(WrapperStats(state), cast_vector, append_count);
	}
	count += append_count;
}

void NumericColumnData::FinalizeAppend(ColumnDataFinalizeAppendState &finalize_state, ColumnAppendState &state) {
	// the inner column flushes its segment statistics; the column and table statistics take the exact statistics
	// kept during the append, which cover the rows of every representation the append went through
	auto current = GetInner();
	auto inner_stats = BaseStatistics::CreateEmpty(current->type);
	ColumnDataFinalizeAppendState inner_finalize(inner_stats);
	current->FinalizeAppend(inner_finalize, state);
	auto &appended = WrapperStats(state);
	appended.CopyValidity(inner_stats);
	for (auto &stats_ref : finalize_state.global_stats) {
		stats_ref.get().Merge(appended);
	}
}

void NumericColumnData::RevertAppend(row_t new_count) {
	GetInner()->RevertAppend(new_count);
	count = UnsafeNumericCast<idx_t>(new_count);
}

//----------------------------------------------------------------------------------------------------------------------
// Fetch
//----------------------------------------------------------------------------------------------------------------------
idx_t NumericColumnData::Fetch(ColumnScanState &state, row_t row_id, Vector &result) {
	auto &column = Pinned(state);
	if (column.type == result.GetType()) {
		return column.Fetch(state, row_id, result);
	}
	Vector intermediate(column.type, STANDARD_VECTOR_SIZE);
	auto fetch_count = column.Fetch(state, row_id, intermediate);
	CastVector(intermediate, result, fetch_count);
	return fetch_count;
}

void NumericColumnData::FetchRows(TransactionData transaction, ColumnFetchState &state,
                                  const StorageIndex &storage_index, const idx_t *offsets, const SelectionVector &sel,
                                  idx_t fetch_count, Vector &result, idx_t result_offset) {
	auto column = GetInner();
	if (column->type == result.GetType()) {
		column->FetchRows(transaction, state, storage_index, offsets, sel, fetch_count, result, result_offset);
		return;
	}
	Vector intermediate(column->type, fetch_count);
	column->FetchRows(transaction, state, storage_index, offsets, sel, fetch_count, intermediate, 0);
	Vector cast_vector(result.GetType(), fetch_count);
	CastVector(intermediate, cast_vector, fetch_count);
	VectorOperations::Copy(cast_vector, result, fetch_count, 0, result_offset);
}

//----------------------------------------------------------------------------------------------------------------------
// Update
//----------------------------------------------------------------------------------------------------------------------
static void CastUpdateVector(Vector &update_vector, Vector &cast_vector, idx_t update_count,
                             const LogicalType &storage_type) {
	string error_message;
	if (!VectorOperations::DefaultTryCast(update_vector, cast_vector, update_count, &error_message, true)) {
		throw NotImplementedException("UPDATE of a NUMERIC column with values that do not fit its current storage "
		                              "type %s is not supported yet",
		                              storage_type.ToString());
	}
}

void NumericColumnData::Update(TransactionData transaction, DuckTableEntry &table_entry, idx_t column_index_p,
                               Vector &update_vector, row_t *row_ids, idx_t update_count, idx_t row_group_start) {
	auto column = GetInner();
	if (update_vector.GetType() == column->type) {
		column->Update(transaction, table_entry, column_index_p, update_vector, row_ids, update_count, row_group_start);
		return;
	}
	Vector cast_vector(column->type, update_count);
	CastUpdateVector(update_vector, cast_vector, update_count, column->type);
	column->Update(transaction, table_entry, column_index_p, cast_vector, row_ids, update_count, row_group_start);
}

void NumericColumnData::UpdateColumn(TransactionData transaction, DuckTableEntry &table_entry,
                                     const vector<column_t> &column_path, Vector &update_vector, row_t *row_ids,
                                     idx_t update_count, idx_t depth, idx_t row_group_start) {
	auto column = GetInner();
	if (update_vector.GetType() == column->type) {
		column->UpdateColumn(transaction, table_entry, column_path, update_vector, row_ids, update_count, depth,
		                     row_group_start);
		return;
	}
	Vector cast_vector(column->type, update_count);
	CastUpdateVector(update_vector, cast_vector, update_count, column->type);
	column->UpdateColumn(transaction, table_entry, column_path, cast_vector, row_ids, update_count, depth,
	                     row_group_start);
}

unique_ptr<BaseStatistics> NumericColumnData::GetUpdateStatistics() {
	return GetInner()->GetUpdateStatistics();
}

//----------------------------------------------------------------------------------------------------------------------
// Checkpoint
//----------------------------------------------------------------------------------------------------------------------
class NumericColumnCheckpointState final : public ColumnCheckpointState {
public:
	NumericColumnCheckpointState(const RowGroup &row_group, ColumnData &column_data,
	                             PartialBlockManager &partial_block_manager)
	    : ColumnCheckpointState(row_group, column_data, partial_block_manager) {
		global_stats = UnboundedNumericStats::CreateEmpty(column_data.type).ToUnique();
	}

	//! The (possibly rebuilt) inner column that was checkpointed and its checkpoint state
	shared_ptr<ColumnData> inner_column;
	unique_ptr<ColumnCheckpointState> inner_state;

	shared_ptr<ColumnData> CreateEmptyColumnData() override {
		return make_shared_ptr<NumericColumnData>(original_column.GetBlockManager(), original_column.GetTableInfo(),
		                                          original_column.column_index, original_column.type,
		                                          ColumnDataType::CHECKPOINT_TARGET, nullptr);
	}

	shared_ptr<ColumnData> GetFinalResult() override {
		if (!result_column) {
			result_column = CreateEmptyColumnData();
		}
		auto &column_data = result_column->Cast<NumericColumnData>();
		column_data.SetInner(inner_state->GetFinalResult());
		return ColumnCheckpointState::GetFinalResult();
	}

	unique_ptr<BaseStatistics> GetStatistics() override {
		D_ASSERT(global_stats);
		return std::move(global_stats);
	}

	//! The exact statistics of the row group, kept for the persistent data after GetStatistics() moved global_stats
	unique_ptr<BaseStatistics> persistent_stats;

	PersistentColumnData ToPersistentData() override {
		auto inner_data = inner_state->ToPersistentData();
		inner_data.extra_data = make_uniq<NumericPersistentColumnData>(
		    inner_column->type, persistent_stats ? persistent_stats->ToUnique() : nullptr);
		return inner_data;
	}
};

unique_ptr<ColumnCheckpointState> NumericColumnData::CreateCheckpointState(const RowGroup &row_group,
                                                                           PartialBlockManager &partial_block_manager) {
	return make_uniq<NumericColumnCheckpointState>(row_group, *this, partial_block_manager);
}

unique_ptr<ColumnCheckpointState> NumericColumnData::Checkpoint(const RowGroup &row_group, ColumnCheckpointInfo &info,
                                                                const BaseStatistics &old_stats) {
	auto checkpoint_state = make_uniq<NumericColumnCheckpointState>(row_group, *this, info.GetPartialBlockManager());
	auto current = GetInner();
	auto target_column = current;
	if (HasAnyChanges() || !IsPersistent()) {
		// store the row group in the narrowest representation that fits its values
		auto target_type = UnboundedNumericStats::ResolveType(old_stats);
		if (target_type != current->type) {
			target_column = Rebuild(*current, target_type);
		}
	}
	// the inner column expects statistics of its own type
	auto typed_old_stats = UnboundedNumericStats::ToTyped(old_stats, target_column->type);
	checkpoint_state->inner_column = target_column;
	checkpoint_state->inner_state = target_column->Checkpoint(row_group, info, typed_old_stats);
	// the column statistics are exact for every representation - the segment statistics of wide decimals are not
	auto inner_stats = checkpoint_state->inner_state->GetStatistics();
	checkpoint_state->global_stats = old_stats.ToUnique();
	checkpoint_state->global_stats->CopyValidity(*inner_stats);
	checkpoint_state->persistent_stats = checkpoint_state->global_stats->ToUnique();
	return std::move(checkpoint_state);
}

bool NumericColumnData::IsPersistent() {
	return GetInner()->IsPersistent();
}

bool NumericColumnData::HasAnyChanges() const {
	return GetInner()->HasAnyChanges();
}

PersistentColumnData NumericColumnData::Serialize() {
	auto column = GetInner();
	auto inner_data = column->Serialize();
	// a nested NUMERIC column keeps its statistics in its parent
	inner_data.extra_data = make_uniq<NumericPersistentColumnData>(column->type, GetStatisticsRef().ToUnique());
	return inner_data;
}

void NumericColumnData::InitializeColumn(PersistentColumnData &column_data, BaseStatistics &target_stats) {
	if (!column_data.extra_data || column_data.extra_data->GetType() != ExtraPersistentColumnDataType::NUMERIC) {
		throw DataCorruptionException("NUMERIC column data without a storage type");
	}
	auto &numeric_data = column_data.extra_data->Cast<NumericPersistentColumnData>();
	auto new_inner = CreateInner(numeric_data.storage_type);
	auto typed_stats = BaseStatistics::CreateEmpty(numeric_data.storage_type);
	new_inner->InitializeColumn(column_data, typed_stats);
	{
		lock_guard<mutex> guard(inner_lock);
		inner = std::move(new_inner);
	}
	count = inner->count.load();
	if (numeric_data.stats) {
		// the exact value range written with the row group
		target_stats.Merge(*numeric_data.stats);
	} else {
		target_stats.Merge(typed_stats);
	}
}

//----------------------------------------------------------------------------------------------------------------------
// Misc
//----------------------------------------------------------------------------------------------------------------------
void NumericColumnData::GetColumnSegmentInfo(const QueryContext &context, idx_t row_group_index, vector<idx_t> col_path,
                                             vector<ColumnSegmentInfo> &result,
                                             const ColumnSegmentInfoScanOptions &options) {
	GetInner()->GetColumnSegmentInfo(context, row_group_index, std::move(col_path), result, options);
}

void NumericColumnData::Verify(RowGroup &parent) {
	GetInner()->Verify(parent);
}

void NumericColumnData::VisitBlockIds(BlockIdVisitor &visitor) const {
	GetInner()->VisitBlockIds(visitor);
}

} // namespace duckdb
