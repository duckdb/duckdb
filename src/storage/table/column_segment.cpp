#include "duckdb/storage/table/column_segment.hpp"

#include "duckdb/common/limits.hpp"
#include "duckdb/common/types/null_value.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/filter/struct_filter.hpp"
#include "duckdb/storage/data_pointer.hpp"
#include "duckdb/storage/storage_manager.hpp"
#include "duckdb/storage/table/append_state.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/storage/table/update_segment.hpp"
#include "duckdb/planner/table_filter_state.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"

#include <cstring>

namespace duckdb {

//===--------------------------------------------------------------------===//
// Create
//===--------------------------------------------------------------------===//

unique_ptr<ColumnSegment> ColumnSegment::CreatePersistentSegment(DatabaseInstance &db, BlockManager &block_manager,
                                                                 block_id_t block_id, idx_t offset,
                                                                 const LogicalType &type, idx_t count,
                                                                 CompressionType compression_type,
                                                                 BaseStatistics statistics,
                                                                 unique_ptr<ColumnSegmentState> segment_state) {
	auto &config = DBConfig::GetConfig(db);
	optional_ptr<CompressionFunction> function;
	shared_ptr<BlockHandle> block;

	function = config.GetCompressionFunction(compression_type, type.InternalType());
	if (block_id != INVALID_BLOCK) {
		block = block_manager.RegisterBlock(block_id);
	}

	auto segment_size = block_manager.GetBlockSize();
	return make_uniq<ColumnSegment>(db, std::move(block), type, ColumnSegmentType::PERSISTENT, count, *function,
	                                std::move(statistics), block_id, offset, segment_size, std::move(segment_state));
}

unique_ptr<ColumnSegment> ColumnSegment::CreateTransientSegment(DatabaseInstance &db, CompressionFunction &function,
                                                                const LogicalType &type, const idx_t segment_size,
                                                                BlockManager &block_manager) {
	// Allocate a buffer for the uncompressed segment.
	auto &buffer_manager = BufferManager::GetBufferManager(db);
	D_ASSERT(&buffer_manager == &block_manager.buffer_manager);
	auto block = buffer_manager.RegisterTransientMemory(segment_size, block_manager);

	return make_uniq<ColumnSegment>(db, std::move(block), type, ColumnSegmentType::TRANSIENT, 0U, function,
	                                BaseStatistics::CreateEmpty(type), INVALID_BLOCK, 0U, segment_size);
}

//===--------------------------------------------------------------------===//
// Construct/Destruct
//===--------------------------------------------------------------------===//
ColumnSegment::ColumnSegment(DatabaseInstance &db, shared_ptr<BlockHandle> block_p, const LogicalType &type,
                             const ColumnSegmentType segment_type, const idx_t count, CompressionFunction &function_p,
                             BaseStatistics statistics, const block_id_t block_id_p, const idx_t offset,
                             const idx_t segment_size_p, const unique_ptr<ColumnSegmentState> segment_state_p)

    : SegmentBase<ColumnSegment>(count), db(db), type(type), type_size(GetTypeIdSize(type.InternalType())),
      segment_type(segment_type), stats(std::move(statistics)), block(std::move(block_p)), function(function_p),
      block_id(block_id_p), offset(offset), segment_size(segment_size_p) {
	if (function.get().init_segment) {
		segment_state = function.get().init_segment(*this, block_id, segment_state_p.get());
	}

	// For constant segments (CompressionType::COMPRESSION_CONSTANT) the block is a nullptr.
	D_ASSERT(!block || segment_size <= GetBlockManager().GetBlockSize());
}

ColumnSegment::ColumnSegment(ColumnSegment &other)
    : SegmentBase<ColumnSegment>(other.count.load()), db(other.db), type(std::move(other.type)),
      type_size(other.type_size), segment_type(other.segment_type), stats(std::move(other.stats)),
      block(std::move(other.block)), function(other.function), block_id(other.block_id), offset(other.offset),
      segment_size(other.segment_size), segment_state(std::move(other.segment_state)) {
	// For constant segments (CompressionType::COMPRESSION_CONSTANT) the block is a nullptr.
	D_ASSERT(!block || segment_size <= GetBlockManager().GetBlockSize());
}

ColumnSegment::~ColumnSegment() {
}

//===--------------------------------------------------------------------===//
// Scan
//===--------------------------------------------------------------------===//
void ColumnSegment::InitializePrefetch(PrefetchState &prefetch_state, ColumnScanState &) {
	if (!block || block->BlockId() >= MAXIMUM_BLOCK) {
		// not an on-disk block
		return;
	}
	if (function.get().init_prefetch) {
		function.get().init_prefetch(*this, prefetch_state);
	} else {
		prefetch_state.AddBlock(block);
	}
}

void ColumnSegment::InitializeScan(ColumnScanState &state) {
	state.scan_state = function.get().init_scan(state.context, *this);
}

void ColumnSegment::Scan(ColumnScanState &state, idx_t scan_count, Vector &result, idx_t result_offset,
                         ScanVectorType scan_type) {
	if (scan_type == ScanVectorType::SCAN_ENTIRE_VECTOR) {
		D_ASSERT(result_offset == 0);
		Scan(state, scan_count, result);
	} else {
		D_ASSERT(result.GetVectorType() == VectorType::FLAT_VECTOR);
		ScanPartial(state, scan_count, result, result_offset);
		D_ASSERT(result.GetVectorType() == VectorType::FLAT_VECTOR);
	}
}

void ColumnSegment::Select(ColumnScanState &state, idx_t scan_count, Vector &result, const SelectionVector &sel,
                           idx_t sel_count) {
	if (!function.get().select) {
		throw InternalException("ColumnSegment::Select not implemented for this compression method");
	}
	function.get().select(*this, state, scan_count, result, sel, sel_count);
}

void ColumnSegment::Filter(ColumnScanState &state, idx_t scan_count, Vector &result, SelectionVector &sel,
                           idx_t &sel_count, const TableFilter &filter, TableFilterState &filter_state) {
	if (!function.get().filter) {
		throw InternalException("ColumnSegment::Filter not implemented for this compression method");
	}
	function.get().filter(*this, state, scan_count, result, sel, sel_count, filter, filter_state);
}

void ColumnSegment::Skip(ColumnScanState &state) {
	function.get().skip(*this, state, state.offset_in_column - state.internal_index);
	state.internal_index = state.offset_in_column;
}

void ColumnSegment::Scan(ColumnScanState &state, idx_t scan_count, Vector &result) {
	function.get().scan_vector(*this, state, scan_count, result);
}

void ColumnSegment::ScanPartial(ColumnScanState &state, idx_t scan_count, Vector &result, idx_t result_offset) {
	function.get().scan_partial(*this, state, scan_count, result, result_offset);
}

//===--------------------------------------------------------------------===//
// Fetch
//===--------------------------------------------------------------------===//
void ColumnSegment::FetchRow(ColumnFetchState &state, row_t row_id, Vector &result, idx_t result_idx) {
	if (UnsafeNumericCast<idx_t>(row_id) > count) {
		throw InternalException("ColumnSegment::FetchRow - row_id out of range for segment");
	}
	function.get().fetch_row(*this, state, row_id, result, result_idx);
}

//===--------------------------------------------------------------------===//
// Append
//===--------------------------------------------------------------------===//
idx_t ColumnSegment::SegmentSize() const {
	return segment_size;
}

void ColumnSegment::Resize(idx_t new_size) {
	D_ASSERT(new_size > segment_size);
	D_ASSERT(offset == 0);
	D_ASSERT(block && new_size <= GetBlockManager().GetBlockSize());

	auto &buffer_manager = BufferManager::GetBufferManager(db);
	auto old_handle = buffer_manager.Pin(block);
	auto new_handle = buffer_manager.Allocate(MemoryTag::IN_MEMORY_TABLE, new_size);
	auto new_block = new_handle.GetBlockHandle();
	memcpy(new_handle.Ptr(), old_handle.Ptr(), segment_size);

	this->block_id = new_block->BlockId();
	this->block = std::move(new_block);
	this->segment_size = new_size;
}

void ColumnSegment::InitializeAppend(ColumnAppendState &state) {
	D_ASSERT(segment_type == ColumnSegmentType::TRANSIENT);
	if (!function.get().init_append) {
		throw InternalException("Attempting to init append to a segment without init_append method");
	}
	state.append_state = function.get().init_append(*this);
}

idx_t ColumnSegment::Append(ColumnAppendState &state, UnifiedVectorFormat &append_data, idx_t offset, idx_t count) {
	D_ASSERT(segment_type == ColumnSegmentType::TRANSIENT);
	if (!function.get().append) {
		throw InternalException("Attempting to append to a segment without append method");
	}
	return function.get().append(*state.append_state, *this, stats, append_data, offset, count);
}

idx_t ColumnSegment::FinalizeAppend(ColumnAppendState &state) {
	D_ASSERT(segment_type == ColumnSegmentType::TRANSIENT);
	if (!function.get().finalize_append) {
		throw InternalException("Attempting to call FinalizeAppend on a segment without a finalize_append method");
	}
	auto result_count = function.get().finalize_append(*this, stats);
	state.append_state.reset();
	return result_count;
}

void ColumnSegment::RevertAppend(idx_t new_count) {
	D_ASSERT(segment_type == ColumnSegmentType::TRANSIENT);
	if (function.get().revert_append) {
		function.get().revert_append(*this, new_count);
	}
	this->count = new_count;
}

//===--------------------------------------------------------------------===//
// Convert To Persistent
//===--------------------------------------------------------------------===//
void ColumnSegment::ConvertToPersistent(QueryContext context, optional_ptr<BlockManager> block_manager,
                                        const block_id_t block_id_p) {
	D_ASSERT(segment_type == ColumnSegmentType::TRANSIENT);
	segment_type = ColumnSegmentType::PERSISTENT;
	block_id = block_id_p;
	offset = 0;

	if (block_id != INVALID_BLOCK) {
		D_ASSERT(!stats.statistics.IsConstant());
		// Non-constant block: write the block to disk.
		// The block data already exists in memory, so we alter the metadata,
		// which ensures that the buffer points to an on-disk block.
		block = block_manager->ConvertToPersistent(context, block_id, std::move(block));
		return;
	}

	// Constant block: no need to write anything to disk besides the stats (metadata).
	// I.e., we do not need to write an actual block.
	// Thus, we set the compression function to constant and reset the block buffer.
	D_ASSERT(stats.statistics.IsConstant());
	auto &config = DBConfig::GetConfig(db);
	function = *config.GetCompressionFunction(CompressionType::COMPRESSION_CONSTANT, type.InternalType());
	block.reset();
}

void ColumnSegment::MarkAsPersistent(shared_ptr<BlockHandle> block_p, uint32_t offset_p) {
	D_ASSERT(segment_type == ColumnSegmentType::TRANSIENT);
	block_id = block_p->BlockId();
	SetBlock(std::move(block_p), offset_p);
}

void ColumnSegment::SetBlock(shared_ptr<BlockHandle> block_p, uint32_t offset_p) {
	segment_type = ColumnSegmentType::PERSISTENT;
	offset = offset_p;
	block = std::move(block_p);
}

DataPointer ColumnSegment::GetDataPointer(idx_t row_start) {
	if (segment_type != ColumnSegmentType::PERSISTENT) {
		throw InternalException("Attempting to call ColumnSegment::GetDataPointer on a transient segment");
	}
	// set up the data pointer directly using the data from the persistent segment
	DataPointer pointer(stats.statistics.Copy());
	pointer.block_pointer.block_id = GetBlockId();
	pointer.block_pointer.offset = NumericCast<uint32_t>(GetBlockOffset());
	pointer.row_start = row_start;
	pointer.tuple_count = count;
	pointer.compression_type = function.get().type;
	if (function.get().serialize_state) {
		pointer.segment_state = function.get().serialize_state(*this);
	}
	return pointer;
}

//===--------------------------------------------------------------------===//
// Drop Segment
//===--------------------------------------------------------------------===//
void ColumnSegment::CommitDropSegment() {
	if (block_id != INVALID_BLOCK) {
		GetBlockManager().MarkBlockAsModified(block_id);
	}
	if (function.get().cleanup_state) {
		function.get().cleanup_state(*this);
	}
}

//===--------------------------------------------------------------------===//
// Filter Selection
//===--------------------------------------------------------------------===//
template <class T, class OP, bool HAS_NULL>
static idx_t TemplatedFilterSelection(UnifiedVectorFormat &vdata, T predicate, SelectionVector &sel,
                                      idx_t approved_tuple_count, SelectionVector &result_sel) {
	auto &mask = vdata.validity;
	auto vec = UnifiedVectorFormat::GetData<T>(vdata);
	idx_t result_count = 0;
	for (idx_t i = 0; i < approved_tuple_count; i++) {
		auto idx = sel.get_index(i);
		auto vector_idx = vdata.sel->get_index(idx);
		bool comparison_result =
		    (!HAS_NULL || mask.RowIsValid(vector_idx)) && OP::Operation(vec[vector_idx], predicate);
		result_sel.set_index(result_count, idx);
		result_count += comparison_result;
	}
	return result_count;
}

template <class T>
static void FilterSelectionSwitch(UnifiedVectorFormat &vdata, T predicate, SelectionVector &sel,
                                  idx_t &approved_tuple_count, ExpressionType comparison_type) {
	SelectionVector new_sel(approved_tuple_count);
	auto &mask = vdata.validity;
	// the inplace loops take the result as the last parameter
	switch (comparison_type) {
	case ExpressionType::COMPARE_EQUAL: {
		if (mask.AllValid()) {
			approved_tuple_count =
			    TemplatedFilterSelection<T, Equals, false>(vdata, predicate, sel, approved_tuple_count, new_sel);
		} else {
			approved_tuple_count =
			    TemplatedFilterSelection<T, Equals, true>(vdata, predicate, sel, approved_tuple_count, new_sel);
		}
		break;
	}
	case ExpressionType::COMPARE_NOTEQUAL: {
		if (mask.AllValid()) {
			approved_tuple_count =
			    TemplatedFilterSelection<T, NotEquals, false>(vdata, predicate, sel, approved_tuple_count, new_sel);
		} else {
			approved_tuple_count =
			    TemplatedFilterSelection<T, NotEquals, true>(vdata, predicate, sel, approved_tuple_count, new_sel);
		}
		break;
	}
	case ExpressionType::COMPARE_LESSTHAN: {
		if (mask.AllValid()) {
			approved_tuple_count =
			    TemplatedFilterSelection<T, LessThan, false>(vdata, predicate, sel, approved_tuple_count, new_sel);
		} else {
			approved_tuple_count =
			    TemplatedFilterSelection<T, LessThan, true>(vdata, predicate, sel, approved_tuple_count, new_sel);
		}
		break;
	}
	case ExpressionType::COMPARE_GREATERTHAN: {
		if (mask.AllValid()) {
			approved_tuple_count =
			    TemplatedFilterSelection<T, GreaterThan, false>(vdata, predicate, sel, approved_tuple_count, new_sel);
		} else {
			approved_tuple_count =
			    TemplatedFilterSelection<T, GreaterThan, true>(vdata, predicate, sel, approved_tuple_count, new_sel);
		}
		break;
	}
	case ExpressionType::COMPARE_LESSTHANOREQUALTO: {
		if (mask.AllValid()) {
			approved_tuple_count = TemplatedFilterSelection<T, LessThanEquals, false>(vdata, predicate, sel,
			                                                                          approved_tuple_count, new_sel);
		} else {
			approved_tuple_count =
			    TemplatedFilterSelection<T, LessThanEquals, true>(vdata, predicate, sel, approved_tuple_count, new_sel);
		}
		break;
	}
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO: {
		if (mask.AllValid()) {
			approved_tuple_count = TemplatedFilterSelection<T, GreaterThanEquals, false>(vdata, predicate, sel,
			                                                                             approved_tuple_count, new_sel);
		} else {
			approved_tuple_count = TemplatedFilterSelection<T, GreaterThanEquals, true>(vdata, predicate, sel,
			                                                                            approved_tuple_count, new_sel);
		}
		break;
	}
	default:
		throw NotImplementedException("Unknown comparison type for filter pushed down to table!");
	}
	sel.Initialize(new_sel);
}

template <bool IS_NULL>
static idx_t TemplatedNullSelection(UnifiedVectorFormat &vdata, SelectionVector &sel, idx_t &approved_tuple_count) {
	auto &mask = vdata.validity;
	if (mask.AllValid()) {
		// no NULL values
		if (IS_NULL) {
			approved_tuple_count = 0;
			return 0;
		} else {
			return approved_tuple_count;
		}
	} else {
		SelectionVector result_sel(approved_tuple_count);
		idx_t result_count = 0;
		for (idx_t i = 0; i < approved_tuple_count; i++) {
			auto idx = sel.get_index(i);
			auto vector_idx = vdata.sel->get_index(idx);
			if (mask.RowIsValid(vector_idx) != IS_NULL) {
				result_sel.set_index(result_count++, idx);
			}
		}
		sel.Initialize(result_sel);
		approved_tuple_count = result_count;
		return result_count;
	}
}

idx_t ColumnSegment::FilterSelection(SelectionVector &sel, Vector &vector, UnifiedVectorFormat &vdata,
                                     const TableFilter &filter, TableFilterState &filter_state, idx_t scan_count,
                                     idx_t &approved_tuple_count) {
	switch (filter.filter_type) {
	case TableFilterType::OPTIONAL_FILTER: {
		return scan_count;
	}
	case TableFilterType::CONJUNCTION_OR: {
		// similar to the CONJUNCTION_AND, but we need to take care of the SelectionVectors (OR all of them)
		auto &state = filter_state.Cast<ConjunctionOrFilterState>();
		idx_t count_total = 0;
		SelectionVector result_sel(approved_tuple_count);
		auto &conjunction_or = filter.Cast<ConjunctionOrFilter>();
		for (idx_t child_idx = 0; child_idx < conjunction_or.child_filters.size(); child_idx++) {
			auto &child_filter = *conjunction_or.child_filters[child_idx];
			SelectionVector temp_sel;
			temp_sel.Initialize(sel);
			idx_t temp_tuple_count = approved_tuple_count;
			idx_t temp_count = FilterSelection(temp_sel, vector, vdata, child_filter, *state.child_states[child_idx],
			                                   scan_count, temp_tuple_count);
			// tuples passed, move them into the actual result vector
			for (idx_t i = 0; i < temp_count; i++) {
				auto new_idx = temp_sel.get_index(i);
				bool is_new_idx = true;
				for (idx_t res_idx = 0; res_idx < count_total; res_idx++) {
					if (result_sel.get_index(res_idx) == new_idx) {
						is_new_idx = false;
						break;
					}
				}
				if (is_new_idx) {
					result_sel.set_index(count_total++, new_idx);
				}
			}
		}
		sel.Initialize(result_sel);
		approved_tuple_count = count_total;
		return approved_tuple_count;
	}
	case TableFilterType::CONJUNCTION_AND: {
		auto &conjunction_and = filter.Cast<ConjunctionAndFilter>();
		auto &state = filter_state.Cast<ConjunctionAndFilterState>();
		for (idx_t child_idx = 0; child_idx < conjunction_and.child_filters.size(); child_idx++) {
			auto &child_filter = *conjunction_and.child_filters[child_idx];
			FilterSelection(sel, vector, vdata, child_filter, *state.child_states[child_idx], scan_count,
			                approved_tuple_count);
		}
		return approved_tuple_count;
	}
	case TableFilterType::CONSTANT_COMPARISON: {
		auto &constant_filter = filter.Cast<ConstantFilter>();
		switch (vector.GetType().InternalType()) {
		case PhysicalType::UINT8: {
			auto predicate = UTinyIntValue::Get(constant_filter.constant);
			FilterSelectionSwitch<uint8_t>(vdata, predicate, sel, approved_tuple_count,
			                               constant_filter.comparison_type);
			break;
		}
		case PhysicalType::UINT16: {
			auto predicate = USmallIntValue::Get(constant_filter.constant);
			FilterSelectionSwitch<uint16_t>(vdata, predicate, sel, approved_tuple_count,
			                                constant_filter.comparison_type);
			break;
		}
		case PhysicalType::UINT32: {
			auto predicate = UIntegerValue::Get(constant_filter.constant);
			FilterSelectionSwitch<uint32_t>(vdata, predicate, sel, approved_tuple_count,
			                                constant_filter.comparison_type);
			break;
		}
		case PhysicalType::UINT64: {
			auto predicate = UBigIntValue::Get(constant_filter.constant);
			FilterSelectionSwitch<uint64_t>(vdata, predicate, sel, approved_tuple_count,
			                                constant_filter.comparison_type);
			break;
		}
		case PhysicalType::INT8: {
			auto predicate = TinyIntValue::Get(constant_filter.constant);
			FilterSelectionSwitch<int8_t>(vdata, predicate, sel, approved_tuple_count, constant_filter.comparison_type);
			break;
		}
		case PhysicalType::INT16: {
			auto predicate = SmallIntValue::Get(constant_filter.constant);
			FilterSelectionSwitch<int16_t>(vdata, predicate, sel, approved_tuple_count,
			                               constant_filter.comparison_type);
			break;
		}
		case PhysicalType::INT32: {
			auto predicate = IntegerValue::Get(constant_filter.constant);
			FilterSelectionSwitch<int32_t>(vdata, predicate, sel, approved_tuple_count,
			                               constant_filter.comparison_type);
			break;
		}
		case PhysicalType::INT64: {
			auto predicate = BigIntValue::Get(constant_filter.constant);
			FilterSelectionSwitch<int64_t>(vdata, predicate, sel, approved_tuple_count,
			                               constant_filter.comparison_type);
			break;
		}
		case PhysicalType::INT128: {
			auto predicate = HugeIntValue::Get(constant_filter.constant);
			FilterSelectionSwitch<hugeint_t>(vdata, predicate, sel, approved_tuple_count,
			                                 constant_filter.comparison_type);
			break;
		}
		case PhysicalType::UINT128: {
			auto predicate = UhugeIntValue::Get(constant_filter.constant);
			FilterSelectionSwitch<uhugeint_t>(vdata, predicate, sel, approved_tuple_count,
			                                  constant_filter.comparison_type);
			break;
		}
		case PhysicalType::FLOAT: {
			auto predicate = FloatValue::Get(constant_filter.constant);
			FilterSelectionSwitch<float>(vdata, predicate, sel, approved_tuple_count, constant_filter.comparison_type);
			break;
		}
		case PhysicalType::DOUBLE: {
			auto predicate = DoubleValue::Get(constant_filter.constant);
			FilterSelectionSwitch<double>(vdata, predicate, sel, approved_tuple_count, constant_filter.comparison_type);
			break;
		}
		case PhysicalType::VARCHAR: {
			auto predicate = string_t(StringValue::Get(constant_filter.constant));
			FilterSelectionSwitch<string_t>(vdata, predicate, sel, approved_tuple_count,
			                                constant_filter.comparison_type);
			break;
		}
		case PhysicalType::BOOL: {
			auto predicate = BooleanValue::Get(constant_filter.constant);
			FilterSelectionSwitch<bool>(vdata, predicate, sel, approved_tuple_count, constant_filter.comparison_type);
			break;
		}
		default:
			throw InvalidTypeException(vector.GetType(), "Invalid type for filter pushed down to table comparison");
		}
		return approved_tuple_count;
	}
	case TableFilterType::IS_NULL: {
		return TemplatedNullSelection<true>(vdata, sel, approved_tuple_count);
	}
	case TableFilterType::IS_NOT_NULL: {
		return TemplatedNullSelection<false>(vdata, sel, approved_tuple_count);
	}
	case TableFilterType::STRUCT_EXTRACT: {
		auto &struct_filter = filter.Cast<StructFilter>();
		// Apply the filter on the child vector
		auto &child_vec = StructVector::GetEntries(vector)[struct_filter.child_idx];
		UnifiedVectorFormat child_data;
		child_vec->ToUnifiedFormat(scan_count, child_data);
		return FilterSelection(sel, *child_vec, child_data, *struct_filter.child_filter, filter_state, scan_count,
		                       approved_tuple_count);
	}
	case TableFilterType::EXPRESSION_FILTER: {
		auto &state = filter_state.Cast<ExpressionFilterState>();
		SelectionVector result_sel(approved_tuple_count);
		if (scan_count > STANDARD_VECTOR_SIZE) {
			// scan count is > vector size - split up the vector into multiple chunks
			idx_t offset = 0;
			idx_t result_offset = 0;
			idx_t current_sel_offset = 0;
			SelectionVector current_sel(approved_tuple_count);
			while (offset < scan_count) {
				idx_t chunk_count = MinValue<idx_t>(STANDARD_VECTOR_SIZE, scan_count - offset);
				idx_t chunk_end = offset + chunk_count;
				DataChunk chunk;
				chunk.data.emplace_back(vector, offset, chunk_end);
				chunk.SetCardinality(chunk_count);

				// construct the relevant selection vector for the current chunk (offset ... offset + chunk_count)
				idx_t current_count = 0;
				for (; current_sel_offset < approved_tuple_count; current_sel_offset++) {
					auto sel_index = sel.get_index(current_sel_offset);
					if (sel_index >= chunk_end) {
						// exhausted the chunk
						break;
					}
					if (sel_index < offset) {
						throw InternalException("sel_index < offset in expression filter");
					}
					current_sel.set_index(current_count++, sel_index - offset);
				}
				if (current_count == 0) {
					// no matching tuples in this chunk
					offset += chunk_count;
					continue;
				}
				auto current_result_data = result_sel.data() + result_offset;
				SelectionVector current_result_sel(current_result_data);
				idx_t new_matches =
				    state.executor.SelectExpression(chunk, current_result_sel, current_sel, current_count);
				// increment all matches by the offset
				for (idx_t i = 0; i < new_matches; i++) {
					current_result_data[i] += offset;
				}
				result_offset += new_matches;
				offset += chunk_count;
			}
			approved_tuple_count = result_offset;
		} else {
			// standard case: we can handle everything at once - run the expression once
			DataChunk chunk;
			chunk.data.emplace_back(vector);
			chunk.SetCardinality(scan_count);
			approved_tuple_count = state.executor.SelectExpression(chunk, result_sel, sel, approved_tuple_count);
		}
		sel.Initialize(result_sel);
		return approved_tuple_count;
	}
	default:
		throw InternalException("FIXME: unsupported type for filter selection");
	}
}

const CompressionFunction &ColumnSegment::GetCompressionFunction() {
	return function.get();
}

} // namespace duckdb
