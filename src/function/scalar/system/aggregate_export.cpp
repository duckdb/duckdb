#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/struct_vector.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "duckdb/common/extension_type_info.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/function/scalar/generic_common.hpp"
#include "duckdb/function/scalar/system_functions.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/function/aggregate/distributive_functions.hpp"

namespace duckdb {

namespace {

// aggregate state export
struct ExportAggregateBindData : public FunctionData {
	BoundAggregateFunction aggr;
	unique_ptr<FunctionData> bind_data;
	idx_t state_size;

	explicit ExportAggregateBindData(BoundAggregateFunction aggr_p, unique_ptr<FunctionData> bind_data_p,
	                                 idx_t state_size_p)
	    : aggr(std::move(aggr_p)), bind_data(std::move(bind_data_p)), state_size(state_size_p) {
	}

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<ExportAggregateBindData>(aggr, bind_data ? bind_data->Copy() : nullptr, state_size);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<ExportAggregateBindData>();
		if (bind_data.get() != other.bind_data.get()) {
			if (!bind_data || !other.bind_data) {
				return false;
			}
			if (!bind_data->Equals(*other.bind_data)) {
				return false;
			}
		}
		return aggr == other.aggr && state_size == other.state_size;
	}

	static ExportAggregateBindData &GetFrom(ExpressionState &state) {
		auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
		return func_expr.BindInfo()->Cast<ExportAggregateBindData>();
	}
};

template <class OP, class... ARGS>
void TemplateDispatch(PhysicalType type, ARGS &&... args) {
	switch (type) {
	case PhysicalType::BOOL:
		OP::template Operation<bool>(std::forward<ARGS>(args)...);
		break;
	case PhysicalType::UINT8:
		OP::template Operation<uint8_t>(std::forward<ARGS>(args)...);
		break;
	case PhysicalType::UINT16:
		OP::template Operation<uint16_t>(std::forward<ARGS>(args)...);
		break;
	case PhysicalType::UINT32:
		OP::template Operation<uint32_t>(std::forward<ARGS>(args)...);
		break;
	case PhysicalType::UINT64:
		OP::template Operation<uint64_t>(std::forward<ARGS>(args)...);
		break;
	case PhysicalType::UINT128:
		OP::template Operation<uhugeint_t>(std::forward<ARGS>(args)...);
		break;
	case PhysicalType::INT8:
		OP::template Operation<int8_t>(std::forward<ARGS>(args)...);
		break;
	case PhysicalType::INT16:
		OP::template Operation<int16_t>(std::forward<ARGS>(args)...);
		break;
	case PhysicalType::INT32:
		OP::template Operation<int32_t>(std::forward<ARGS>(args)...);
		break;
	case PhysicalType::INT64:
		OP::template Operation<int64_t>(std::forward<ARGS>(args)...);
		break;
	case PhysicalType::INT128:
		OP::template Operation<hugeint_t>(std::forward<ARGS>(args)...);
		break;
	case PhysicalType::FLOAT:
		OP::template Operation<float>(std::forward<ARGS>(args)...);
		break;
	case PhysicalType::DOUBLE:
		OP::template Operation<double>(std::forward<ARGS>(args)...);
		break;
	case PhysicalType::VARCHAR:
		OP::template Operation<string_t>(std::forward<ARGS>(args)...);
		break;
	case PhysicalType::INTERVAL:
		OP::template Operation<interval_t>(std::forward<ARGS>(args)...);
		break;
	default:
		throw NotImplementedException("Unsupported physical type for default aggregate state export: %s",
		                              TypeIdToString(type));
	}
}

static AggregateStateLayout GetLayout(const BoundAggregateFunction &aggr) {
	return aggr.GetStateTypeCallback()(aggr);
}

/*
 * Load a specific field from the struct aggregate state into the packed binary representation
 * By iterating over rows in the inner loop for a specific type T, the compiler is given a tight loop with a
 * predictable memory access pattern. Since the field in a struct is a contiguous array of type `T`, the compiler
 * can easily vectorize the read operations using SIMD instructions
 */
struct LoadFieldOp {
	template <class T>
	static void Operation(idx_t root_stride, const Vector &struct_vec, idx_t field_idx,
	                      const UnifiedVectorFormat &state_data, idx_t count, data_ptr_t base_ptr, idx_t field_offset) {
		const auto &child = StructVector::GetEntries(struct_vec)[field_idx];
		auto child_data = FlatVector::GetData<T>(child);

		for (idx_t row = 0; row < count; row++) {
			auto row_idx = state_data.sel->get_index(row);
			if (!state_data.validity.RowIsValid(row_idx)) {
				continue;
			}

			auto dest = base_ptr + row * root_stride + field_offset;
			*reinterpret_cast<T *>(dest) = child_data[row];
		}
	}
};

struct StoreFieldOp {
	template <class T>
	static void Operation(Vector &struct_vec, idx_t field_idx, idx_t count, const data_ptr_t *sources,
	                      idx_t field_offset) {
		auto &child = StructVector::GetEntries(struct_vec)[field_idx];
		auto child_data = FlatVector::Writer<T>(child, count);

		for (idx_t row = 0; row < count; row++) {
			auto src = sources[row] + field_offset;
			child_data.WriteValue(*reinterpret_cast<T *>(src));
		}
	}
};

struct CopyFromInputFieldOp {
	template <class T>
	static void Operation(const Vector &input_vec, Vector &result_vec, idx_t field_idx, const SelectionVector &sel,
	                      idx_t count, const UnifiedVectorFormat &input_data) {
		const auto &input_child = StructVector::GetEntries(input_vec)[field_idx];
		auto input_child_data = FlatVector::GetData<T>(input_child);

		auto &result_child = StructVector::GetEntries(result_vec)[field_idx];
		auto result_child_data = FlatVector::GetDataMutable<T>(result_child);

		for (idx_t i = 0; i < count; i++) {
			idx_t row = sel.get_index(i);
			auto src_idx = input_data.sel->get_index(row);
			result_child_data[row] = input_child_data[src_idx];
		}
	}
};

struct LoadFieldForSelectedRowsOp {
	template <class T>
	static void Operation(idx_t total_state_size, const Vector &struct_vec, idx_t field_idx, const SelectionVector &sel,
	                      idx_t count, const UnifiedVectorFormat &state_data, data_ptr_t base_ptr, idx_t field_offset) {
		const auto &child = StructVector::GetEntries(struct_vec)[field_idx];
		auto child_data = FlatVector::GetData<T>(child);

		for (idx_t i = 0; i < count; i++) {
			idx_t row = sel.get_index(i);
			auto src_idx = state_data.sel->get_index(row);
			auto dest = base_ptr + i * total_state_size + field_offset;
			*reinterpret_cast<T *>(dest) = child_data[src_idx];
		}
	}
};

struct StoreFieldForSelectedRowsOp {
	template <class T>
	static void Operation(idx_t total_state_size, Vector &result, idx_t field_idx, const SelectionVector &sel,
	                      idx_t count, data_ptr_t base_ptr, idx_t field_offset) {
		auto &child = StructVector::GetEntries(result)[field_idx];
		auto child_data = FlatVector::ScatterWriter<T>(child);

		for (idx_t i = 0; i < count; i++) {
			idx_t row = sel.get_index(i);
			auto src = base_ptr + i * total_state_size + field_offset;
			child_data[row] = *reinterpret_cast<T *>(src);
		}
	}
};

// Serialize a primitive state buffer (one T per row) into a flat result vector
struct StorePrimitiveOp {
	template <class T>
	static void Operation(Vector &result, idx_t count, const data_ptr_t *addresses) {
		auto result_data = FlatVector::Writer<T>(result, count);
		for (idx_t i = 0; i < count; i++) {
			result_data.WriteValue(*reinterpret_cast<const T *>(addresses[i]));
		}
	}
};

// Deserialize a primitive value from a flat input vector into sequential state buffer slots
struct LoadPrimitiveOp {
	template <class T>
	static void Operation(const Vector &input, const UnifiedVectorFormat &input_data, idx_t count, data_ptr_t base_ptr,
	                      idx_t aligned_state_size) {
		auto src = FlatVector::GetData<T>(input);
		for (idx_t i = 0; i < count; i++) {
			auto src_idx = input_data.sel->get_index(i);
			if (!input_data.validity.RowIsValid(src_idx)) {
				continue;
			}
			*reinterpret_cast<T *>(base_ptr + i * aligned_state_size) = src[src_idx];
		}
	}
};

// Copy a primitive value from selected rows of input to the same rows of result
struct CopyPrimitiveOp {
	template <class T>
	static void Operation(const Vector &input, Vector &result, const SelectionVector &sel, idx_t count,
	                      const UnifiedVectorFormat &input_data) {
		auto src = FlatVector::GetData<T>(input);
		auto dst = FlatVector::GetDataMutable<T>(result);
		for (idx_t i = 0; i < count; i++) {
			idx_t row = sel.get_index(i);
			idx_t src_idx = input_data.sel->get_index(row);
			dst[row] = src[src_idx];
		}
	}
};

// Deserialize selected rows of a flat input vector into sequential state buffer slots
struct LoadPrimitiveForSelectedRowsOp {
	template <class T>
	static void Operation(const Vector &input, const SelectionVector &sel, idx_t count,
	                      const UnifiedVectorFormat &input_data, data_ptr_t base_ptr, idx_t aligned_state_size) {
		auto src = FlatVector::GetData<T>(input);
		for (idx_t i = 0; i < count; i++) {
			idx_t row = sel.get_index(i);
			idx_t src_idx = input_data.sel->get_index(row);
			*reinterpret_cast<T *>(base_ptr + i * aligned_state_size) = src[src_idx];
		}
	}
};

// Serialize sequential state buffer slots into selected rows of a flat result vector
struct StorePrimitiveForSelectedRowsOp {
	template <class T>
	static void Operation(Vector &result, const SelectionVector &sel, idx_t count, data_ptr_t base_ptr,
	                      idx_t aligned_state_size) {
		auto dst = FlatVector::GetDataMutable<T>(result);
		for (idx_t i = 0; i < count; i++) {
			idx_t row = sel.get_index(i);
			dst[row] = *reinterpret_cast<const T *>(base_ptr + i * aligned_state_size);
		}
	}
};

// Serialize optional<T> state buffers into a flat result vector, setting NULL for disengaged optionals
struct StorePrimitiveOptionalOp {
	template <class T>
	static void Operation(Vector &result, idx_t count, const data_ptr_t *addresses) {
		auto dst = FlatVector::GetDataMutable<T>(result);
		for (idx_t i = 0; i < count; i++) {
			const auto &opt = *reinterpret_cast<const optional<T> *>(addresses[i]);
			if (opt.has_value()) {
				dst[i] = *opt;
			} else {
				FlatVector::SetNull(result, i, true);
			}
		}
	}
};

// Deserialize a nullable flat input vector into optional<T> state buffer slots
struct LoadPrimitiveOptionalOp {
	template <class T>
	static void Operation(const Vector &input, const UnifiedVectorFormat &input_data, idx_t count, data_ptr_t base_ptr,
	                      idx_t aligned_state_size) {
		auto src = FlatVector::GetData<T>(input);
		for (idx_t i = 0; i < count; i++) {
			auto src_idx = input_data.sel->get_index(i);
			auto &opt = *reinterpret_cast<optional<T> *>(base_ptr + i * aligned_state_size);
			if (input_data.validity.RowIsValid(src_idx)) {
				opt = src[src_idx];
			} else {
				opt = nullopt;
			}
		}
	}
};

// Copy a nullable primitive value from selected rows of input to the same rows of result
struct CopyPrimitiveOptionalOp {
	template <class T>
	static void Operation(const Vector &input, Vector &result, const SelectionVector &sel, idx_t count,
	                      const UnifiedVectorFormat &input_data) {
		auto src = FlatVector::GetData<T>(input);
		auto dst = FlatVector::GetDataMutable<T>(result);
		for (idx_t i = 0; i < count; i++) {
			idx_t row = sel.get_index(i);
			idx_t src_idx = input_data.sel->get_index(row);
			if (input_data.validity.RowIsValid(src_idx)) {
				dst[row] = src[src_idx];
			} else {
				FlatVector::SetNull(result, row, true);
			}
		}
	}
};

// Deserialize selected rows of a nullable flat input vector into sequential optional<T> state buffer slots
struct LoadPrimitiveOptionalForSelectedRowsOp {
	template <class T>
	static void Operation(const Vector &input, const SelectionVector &sel, idx_t count,
	                      const UnifiedVectorFormat &input_data, data_ptr_t base_ptr, idx_t aligned_state_size) {
		auto src = FlatVector::GetData<T>(input);
		for (idx_t i = 0; i < count; i++) {
			idx_t row = sel.get_index(i);
			idx_t src_idx = input_data.sel->get_index(row);
			auto &opt = *reinterpret_cast<optional<T> *>(base_ptr + i * aligned_state_size);
			if (input_data.validity.RowIsValid(src_idx)) {
				opt = src[src_idx];
			} else {
				opt = nullopt;
			}
		}
	}
};

// Serialize sequential optional<T> state buffer slots into selected rows of a flat result vector
struct StorePrimitiveOptionalForSelectedRowsOp {
	template <class T>
	static void Operation(Vector &result, const SelectionVector &sel, idx_t count, data_ptr_t base_ptr,
	                      idx_t aligned_state_size) {
		auto dst = FlatVector::GetDataMutable<T>(result);
		for (idx_t i = 0; i < count; i++) {
			idx_t row = sel.get_index(i);
			const auto &opt = *reinterpret_cast<const optional<T> *>(base_ptr + i * aligned_state_size);
			if (opt.has_value()) {
				dst[row] = *opt;
			} else {
				FlatVector::SetNull(result, row, true);
			}
		}
	}
};

// Deserialize struct fields from a flattened struct vector into a state buffer.
// Uses LoadFieldOp for tight SIMD-friendly loops.
// root_stride is the aligned size of the top-level state, used to stride between rows in the buffer.
// Child field offsets are pre-computed in AggregateStateField.field_offset (relative to dest_buffer).
void DeserializeStructFields(const LogicalType &type, const AggregateStateField &field, idx_t root_stride,
                             const Vector &struct_vec, const UnifiedVectorFormat &state_data, idx_t count,
                             data_ptr_t dest_buffer) {
	const auto &child_types = StructType::GetChildTypes(type);
	for (idx_t field_idx = 0; field_idx < field.children.size(); field_idx++) {
		const auto &child = field.children[field_idx];
		const auto &child_type = child_types[field_idx].second;
		auto physical = child_type.InternalType();

		if (!child.children.empty()) {
			const auto &struct_entries = StructVector::GetEntries(struct_vec);
			DeserializeStructFields(child_type, child, root_stride, struct_entries[field_idx], state_data, count,
			                        dest_buffer + child.field_offset);
		} else {
			TemplateDispatch<LoadFieldOp>(physical, root_stride, struct_vec, field_idx, state_data, count, dest_buffer,
			                              child.field_offset);
		}
	}
}

// Serialize packed binary states into a struct result vector.
// Uses StoreFieldOp for tight SIMD-friendly loops.
// Child field offsets are pre-computed in AggregateStateField.field_offset (relative to addresses_ptrs[row]).
void SerializeStructFields(const LogicalType &type, const AggregateStateField &field, Vector &result, idx_t count,
                           const data_ptr_t *addresses_ptrs) {
	const auto &child_types = StructType::GetChildTypes(type);
	for (idx_t field_idx = 0; field_idx < field.children.size(); field_idx++) {
		const auto &child = field.children[field_idx];
		const auto &child_type = child_types[field_idx].second;
		auto physical = child_type.InternalType();

		if (!child.children.empty()) {
			// Adjust addresses to point to the start of this nested struct field for each row
			Vector child_addresses(LogicalType::POINTER);
			auto child_writer = FlatVector::Writer<data_ptr_t>(child_addresses, count);
			for (idx_t row = 0; row < count; row++) {
				child_writer.WriteValue(addresses_ptrs[row] + child.field_offset);
			}
			auto child_ptrs = FlatVector::GetData<data_ptr_t>(child_addresses);
			auto &struct_entries = StructVector::GetEntries(result);
			SerializeStructFields(child_type, child, struct_entries[field_idx], count, child_ptrs);
		} else {
			TemplateDispatch<StoreFieldOp>(physical, result, field_idx, count, addresses_ptrs, child.field_offset);
		}
	}
}

#ifdef DEBUG
// Internal verification: export all states to struct, import back, finalize, and compare to main path result
// using vector operations. Catches serialization/deserialization bugs for struct-based aggregate state.
void VerifyStructStateRoundtrip(const AggregateStateLayout &layout, const Vector &state_vec, idx_t count,
                                const UnifiedVectorFormat &state_data, const data_ptr_t *state_vec_ptr,
                                const Vector &result, const ExportAggregateBindData &bind_data,
                                AggregateInputData &aggr_input_data) {
	// Build selection of valid state rows
	SelectionVector valid_sel(count);
	idx_t valid_count = 0;
	for (idx_t i = 0; i < count; i++) {
		if (state_data.validity.RowIsValid(state_data.sel->get_index(i))) {
			valid_sel.set_index(valid_count++, i);
		}
	}
	if (valid_count == 0) {
		return;
	}

	// SerializeStructFields: all valid states -> struct vector
	Vector struct_vec(state_vec.GetType(), valid_count);
	unsafe_unique_array<data_ptr_t> addresses_ptrs(make_unsafe_uniq_array<data_ptr_t>(valid_count));
	for (idx_t i = 0; i < valid_count; i++) {
		addresses_ptrs[i] = state_vec_ptr[valid_sel.get_index(i)];
	}
	SerializeStructFields(layout.type, layout.field, struct_vec, valid_count, addresses_ptrs.get());

	// DeserializeStructFields: struct vector -> packed buffer
	unsafe_unique_array<data_t> temp_state_buf(make_unsafe_uniq_array<data_t>(valid_count * layout.total_state_size));
	UnifiedVectorFormat struct_format;
	struct_vec.ToUnifiedFormat(struct_format);
	DeserializeStructFields(layout.type, layout.field, layout.total_state_size, struct_vec, struct_format, valid_count,
	                        temp_state_buf.get());

	// AggregateStateFinalize: packed buffer -> result values
	Vector addresses_vec(LogicalType::POINTER);
	auto addresses_finalize = FlatVector::Writer<data_ptr_t>(addresses_vec, valid_count);
	for (idx_t i = 0; i < valid_count; i++) {
		addresses_finalize.WriteValue(temp_state_buf.get() + i * layout.total_state_size);
	}
	Vector result_roundtrip(result.GetType(), valid_count);
	bind_data.aggr.GetStateFinalizeCallback()(addresses_vec, aggr_input_data, result_roundtrip, valid_count, 0);

	Vector result_slice(result.GetType(), valid_count);
	result_slice.Slice(result, valid_sel, valid_count);
	SelectionVector true_sel(valid_count);
	SelectionVector false_sel(valid_count);
	idx_t match_count =
	    VectorOperations::NotDistinctFrom(result_slice, result_roundtrip, nullptr, valid_count, &true_sel, &false_sel);
	D_ASSERT(match_count == valid_count &&
	         "Struct state roundtrip failed: finalize(serialize->deserialize(state)) != finalize(state). "
	         "Check SerializeStructFields/DeserializeStructFields for this aggregate.");
}
#endif

struct CombineState : public FunctionLocalState {
	idx_t state_size;

	unsafe_unique_array<data_t> state_buffer0, state_buffer1;
	Vector addresses0, addresses1;

	ArenaAllocator allocator;

	explicit CombineState(idx_t state_size_p)
	    : state_size(state_size_p),
	      state_buffer0(make_unsafe_uniq_array<data_t>(STANDARD_VECTOR_SIZE * AlignValue(state_size_p))),
	      state_buffer1(make_unsafe_uniq_array<data_t>(STANDARD_VECTOR_SIZE * AlignValue(state_size_p))),
	      addresses0(LogicalType::POINTER), addresses1(LogicalType::POINTER), allocator(Allocator::DefaultAllocator()) {
	}
};

unique_ptr<FunctionLocalState> InitCombineState(ExpressionState &state, const BoundFunctionExpression &expr,
                                                FunctionData *bind_data_p) {
	auto &bind_data = bind_data_p->Cast<ExportAggregateBindData>();
	return make_uniq<CombineState>(bind_data.state_size);
}

struct FinalizeState : public FunctionLocalState {
	idx_t state_size;
	unsafe_unique_array<data_t> state_buffer;
	Vector addresses;

	ArenaAllocator allocator;

	explicit FinalizeState(idx_t state_size_p)
	    : state_size(state_size_p),
	      state_buffer(make_unsafe_uniq_array<data_t>(STANDARD_VECTOR_SIZE * AlignValue(state_size_p))),
	      addresses(LogicalType::POINTER), allocator(Allocator::DefaultAllocator()) {
	}
};

unique_ptr<FunctionLocalState> InitFinalizeState(ExpressionState &state, const BoundFunctionExpression &expr,
                                                 FunctionData *bind_data_p) {
	auto &bind_data = bind_data_p->Cast<ExportAggregateBindData>();
	return make_uniq<FinalizeState>(bind_data.state_size);
}

void AggregateStateFinalize(DataChunk &input, ExpressionState &state_p, Vector &result) {
	auto &bind_data = ExportAggregateBindData::GetFrom(state_p);
	auto &local_state = ExecuteFunctionState::GetFunctionState(state_p)->Cast<FinalizeState>();
	local_state.allocator.Reset();

	D_ASSERT(bind_data.state_size == bind_data.aggr.GetStateSizeCallback()(bind_data.aggr));
	D_ASSERT(input.data.size() == 1);

	auto layout = GetLayout(bind_data.aggr);

	auto state_vec_writer = FlatVector::Writer<data_ptr_t>(local_state.addresses, input.size());

	input.data[0].Flatten();

	UnifiedVectorFormat state_data;
	input.data[0].ToUnifiedFormat(state_data);

	for (idx_t i = 0; i < input.size(); i++) {
		state_vec_writer.WriteValue(local_state.state_buffer.get() + i * layout.total_state_size);
	}

	if (layout.type.id() == LogicalTypeId::STRUCT) {
		DeserializeStructFields(layout.type, layout.field, layout.total_state_size, input.data[0], state_data,
		                        input.size(), local_state.state_buffer.get());
	} else if (layout.field.is_optional) {
		TemplateDispatch<LoadPrimitiveOptionalOp>(layout.type.InternalType(), input.data[0], state_data, input.size(),
		                                          local_state.state_buffer.get(), layout.total_state_size);
	} else {
		TemplateDispatch<LoadPrimitiveOp>(layout.type.InternalType(), input.data[0], state_data, input.size(),
		                                  local_state.state_buffer.get(), layout.total_state_size);
	}

	AggregateInputData aggr_input_data(bind_data.aggr, bind_data.bind_data.get(), local_state.allocator);
	bind_data.aggr.GetStateFinalizeCallback()(local_state.addresses, aggr_input_data, result, input.size(), 0);

	for (idx_t i = 0; i < input.size(); i++) {
		auto state_idx = state_data.sel->get_index(i);
		if (!state_data.validity.RowIsValid(state_idx)) {
			FlatVector::SetNull(result, i, true);
		}
	}

#ifdef DEBUG
	if (layout.type.id() == LogicalTypeId::STRUCT) {
		auto state_vec_ptr = FlatVector::GetData<data_ptr_t>(local_state.addresses);
		VerifyStructStateRoundtrip(layout, input.data[0], input.size(), state_data, state_vec_ptr, result, bind_data,
		                           aggr_input_data);
	}
#endif
}

void AggregateStateCombine(DataChunk &input, ExpressionState &state_p, Vector &result) {
	auto &bind_data = ExportAggregateBindData::GetFrom(state_p);
	auto &local_state = ExecuteFunctionState::GetFunctionState(state_p)->Cast<CombineState>();
	local_state.allocator.Reset();

	D_ASSERT(bind_data.state_size == bind_data.aggr.GetStateSizeCallback()(bind_data.aggr));

	D_ASSERT(input.data.size() == 2);
	D_ASSERT(input.data[0].GetType() == result.GetType());

	auto layout = GetLayout(bind_data.aggr);

	if (input.data[0].GetType().InternalType() != input.data[1].GetType().InternalType()) {
		throw IOException("Aggregate state combine type mismatch, expect %s, got %s",
		                  input.data[0].GetType().ToString(), input.data[1].GetType().ToString());
	}

	input.data[0].Flatten();
	input.data[1].Flatten();
	result.Flatten();

	UnifiedVectorFormat state0_data, state1_data;
	input.data[0].ToUnifiedFormat(state0_data);
	input.data[1].ToUnifiedFormat(state1_data);

	// Partition rows by NULL using SelectionVector
	SelectionVector both_null_sel(STANDARD_VECTOR_SIZE);
	// input1 is null
	SelectionVector copy_from_0_sel(STANDARD_VECTOR_SIZE);
	// input0 is null
	SelectionVector copy_from_1_sel(STANDARD_VECTOR_SIZE);
	SelectionVector both_valid_sel(STANDARD_VECTOR_SIZE);

	idx_t both_null_count = 0, copy_from_0_count = 0, copy_from_1_count = 0, both_valid_count = 0;

	for (idx_t i = 0; i < input.size(); i++) {
		const bool is_null0 = !state0_data.validity.RowIsValid(state0_data.sel->get_index(i));
		const bool is_null1 = !state1_data.validity.RowIsValid(state1_data.sel->get_index(i));

		if (is_null0 && is_null1) {
			both_null_sel.set_index(both_null_count++, i);
		} else if (is_null0) {
			copy_from_1_sel.set_index(copy_from_1_count++, i);
		} else if (is_null1) {
			copy_from_0_sel.set_index(copy_from_0_count++, i);
		} else {
			both_valid_sel.set_index(both_valid_count++, i);
		}
	}

	D_ASSERT(both_null_count + copy_from_0_count + copy_from_1_count + both_valid_count == input.size());

	// Handle both-null rows
	for (idx_t i = 0; i < both_null_count; i++) {
		FlatVector::SetNull(result, both_null_sel.get_index(i), true);
	}

	// Handle one-null rows - copy non-null input directly to result
	// copy_from_0: input1 is null, copy input0
	if (copy_from_0_count > 0) {
		if (layout.type.id() == LogicalTypeId::STRUCT) {
			const auto &child_types = StructType::GetChildTypes(layout.type);
			for (idx_t field_idx = 0; field_idx < child_types.size(); field_idx++) {
				auto physical = child_types[field_idx].second.InternalType();
				TemplateDispatch<CopyFromInputFieldOp>(physical, input.data[0], result, field_idx, copy_from_0_sel,
				                                       copy_from_0_count, state0_data);
			}
		} else if (layout.field.is_optional) {
			TemplateDispatch<CopyPrimitiveOptionalOp>(layout.type.InternalType(), input.data[0], result,
			                                          copy_from_0_sel, copy_from_0_count, state0_data);
		} else {
			TemplateDispatch<CopyPrimitiveOp>(layout.type.InternalType(), input.data[0], result, copy_from_0_sel,
			                                  copy_from_0_count, state0_data);
		}
	}
	// copy_from_1: input0 is null, copy input1
	if (copy_from_1_count > 0) {
		if (layout.type.id() == LogicalTypeId::STRUCT) {
			const auto &child_types = StructType::GetChildTypes(layout.type);
			for (idx_t field_idx = 0; field_idx < child_types.size(); field_idx++) {
				auto physical = child_types[field_idx].second.InternalType();
				TemplateDispatch<CopyFromInputFieldOp>(physical, input.data[1], result, field_idx, copy_from_1_sel,
				                                       copy_from_1_count, state1_data);
			}
		} else if (layout.field.is_optional) {
			TemplateDispatch<CopyPrimitiveOptionalOp>(layout.type.InternalType(), input.data[1], result,
			                                          copy_from_1_sel, copy_from_1_count, state1_data);
		} else {
			TemplateDispatch<CopyPrimitiveOp>(layout.type.InternalType(), input.data[1], result, copy_from_1_sel,
			                                  copy_from_1_count, state1_data);
		}
	}

	// Handle both-valid rows - batched load, combine, store
	if (both_valid_count > 0) {
		auto state0_writer = FlatVector::Writer<data_ptr_t>(local_state.addresses0, both_valid_count);
		auto state1_writer = FlatVector::Writer<data_ptr_t>(local_state.addresses1, both_valid_count);

		// Pack state buffer pointers in selection order (not row order)
		for (idx_t i = 0; i < both_valid_count; i++) {
			state0_writer.WriteValue(local_state.state_buffer0.get() + i * layout.total_state_size);
			state1_writer.WriteValue(local_state.state_buffer1.get() + i * layout.total_state_size);
		}

		if (layout.type.id() == LogicalTypeId::STRUCT) {
			// Use tight loops to load both inputs
			const auto &child_types = StructType::GetChildTypes(layout.type);
			for (idx_t field_idx = 0; field_idx < layout.field.children.size(); field_idx++) {
				const auto &child = layout.field.children[field_idx];
				auto physical = child_types[field_idx].second.InternalType();

				TemplateDispatch<LoadFieldForSelectedRowsOp>(physical, layout.total_state_size, input.data[0],
				                                             field_idx, both_valid_sel, both_valid_count, state0_data,
				                                             local_state.state_buffer0.get(), child.field_offset);
				TemplateDispatch<LoadFieldForSelectedRowsOp>(physical, layout.total_state_size, input.data[1],
				                                             field_idx, both_valid_sel, both_valid_count, state1_data,
				                                             local_state.state_buffer1.get(), child.field_offset);
			}
		} else if (layout.field.is_optional) {
			TemplateDispatch<LoadPrimitiveOptionalForSelectedRowsOp>(
			    layout.type.InternalType(), input.data[0], both_valid_sel, both_valid_count, state0_data,
			    local_state.state_buffer0.get(), layout.total_state_size);
			TemplateDispatch<LoadPrimitiveOptionalForSelectedRowsOp>(
			    layout.type.InternalType(), input.data[1], both_valid_sel, both_valid_count, state1_data,
			    local_state.state_buffer1.get(), layout.total_state_size);
		} else {
			TemplateDispatch<LoadPrimitiveForSelectedRowsOp>(layout.type.InternalType(), input.data[0], both_valid_sel,
			                                                 both_valid_count, state0_data,
			                                                 local_state.state_buffer0.get(), layout.total_state_size);
			TemplateDispatch<LoadPrimitiveForSelectedRowsOp>(layout.type.InternalType(), input.data[1], both_valid_sel,
			                                                 both_valid_count, state1_data,
			                                                 local_state.state_buffer1.get(), layout.total_state_size);
		}

		// Single batched combine call
		AggregateInputData aggr_input_data(bind_data.aggr, bind_data.bind_data.get(), local_state.allocator,
		                                   AggregateCombineType::ALLOW_DESTRUCTIVE);
		bind_data.aggr.GetStateCombineCallback()(local_state.addresses0, local_state.addresses1, aggr_input_data,
		                                         both_valid_count);

		// Store results
		if (layout.type.id() == LogicalTypeId::STRUCT) {
			const auto &child_types = StructType::GetChildTypes(layout.type);
			for (idx_t field_idx = 0; field_idx < layout.field.children.size(); field_idx++) {
				const auto &child = layout.field.children[field_idx];
				auto physical = child_types[field_idx].second.InternalType();

				TemplateDispatch<StoreFieldForSelectedRowsOp>(physical, layout.total_state_size, result, field_idx,
				                                              both_valid_sel, both_valid_count,
				                                              local_state.state_buffer1.get(), child.field_offset);
			}
		} else if (layout.field.is_optional) {
			TemplateDispatch<StorePrimitiveOptionalForSelectedRowsOp>(
			    layout.type.InternalType(), result, both_valid_sel, both_valid_count, local_state.state_buffer1.get(),
			    layout.total_state_size);
		} else {
			TemplateDispatch<StorePrimitiveForSelectedRowsOp>(layout.type.InternalType(), result, both_valid_sel,
			                                                  both_valid_count, local_state.state_buffer1.get(),
			                                                  layout.total_state_size);
		}
	}
}

// looks up the aggregate function with the given name in the catalog and binds it with the given argument types
unique_ptr<ExportAggregateBindData> BindExportedAggregate(ClientContext &context, const string &function_name,
                                                          const vector<LogicalType> &argument_types) {
	auto &func = Catalog::GetSystemCatalog(context).GetEntry<AggregateFunctionCatalogEntry>(context, DEFAULT_SCHEMA,
	                                                                                        function_name);
	if (func.type != CatalogType::AGGREGATE_FUNCTION_ENTRY) {
		throw InternalException("Could not find aggregate %s", function_name);
	}
	auto &aggr_entry = func.Cast<AggregateFunctionCatalogEntry>();

	ErrorData error;
	FunctionBinder function_binder(context);
	auto best_function = function_binder.BindFunction(aggr_entry.name, aggr_entry.functions, argument_types, error);
	if (!best_function.IsValid()) {
		throw InternalException("Could not re-bind exported aggregate %s: %s", function_name, error.Message());
	}
	const auto &aggr = aggr_entry.functions.GetFunctionByOffset(best_function.GetIndex());

	// FIXME: this is really hacky
	// but the aggregate state export needs a rework around how it handles more complex aggregates anyway
	vector<unique_ptr<Expression>> args;
	args.reserve(argument_types.size());
	for (auto &arg_type : argument_types) {
		args.push_back(make_uniq<BoundConstantExpression>(Value(arg_type)));
	}

	auto [bound_aggr, bind_info] = function_binder.ResolveFunction(aggr, args);

	if (bound_aggr.GetArguments() != argument_types) {
		throw InternalException("Type mismatch for exported aggregate %s", function_name);
	}

	return make_uniq<ExportAggregateBindData>(bound_aggr, std::move(bind_info),
	                                          bound_aggr.GetStateSizeCallback()(bound_aggr));
}

unique_ptr<ExportAggregateBindData> BindAggregateStateInternal(ClientContext &context, BoundSimpleFunction &function,
                                                               vector<unique_ptr<Expression>> &arguments) {
	auto &arg_return_type = arguments[0]->GetReturnType();
	if (!arg_return_type.IsAggregateState()) {
		throw BinderException("Can only %s %s, not AGGREGATE_STATE", function.GetName(), arg_return_type.ToString());
	}
	//
	// now we can look up the function in the catalog again and bind it
	auto ext_info = arg_return_type.GetExtensionInfo();
	auto entry = ext_info->properties.find("function_name");
	if (entry == ext_info->properties.end() || entry->second.type().id() != LogicalTypeId::VARCHAR ||
	    entry->second.IsNull()) {
		throw InternalException("Aggregate state object should have a property called function_name that is a string");
	}
	auto &function_name = StringValue::Get(entry->second);

	entry = ext_info->properties.find("parameters");
	if (entry == ext_info->properties.end() || entry->second.type().id() != LogicalTypeId::LIST ||
	    entry->second.IsNull()) {
		throw InternalException(
		    "Aggregate state object should have a property called parameters that is a list of types");
	}
	vector<LogicalType> argument_types;
	for (auto &val : ListValue::GetChildren(entry->second)) {
		if (val.IsNull() || val.type().id() != LogicalTypeId::TYPE) {
			throw InternalException(
			    "Aggregate state object should have a property called parameters that is a list of types");
		}
		argument_types.push_back(TypeValue::GetType(val));
	}

	return BindExportedAggregate(context, function_name, argument_types);
}

unique_ptr<FunctionData> BindAggregateState(BindScalarFunctionInput &input) {
	auto &bound_function = input.GetBoundFunction();
	auto &arguments = input.GetArguments();
	auto bind_data =
	    BindAggregateStateInternal(input.GetClientContext(), input.GetBoundFunction(), input.GetArguments());

	// combine - both arguments must be aggregate states of the same function with the same signature
	if (arguments.size() == 2 && arguments[0]->GetReturnType() != arguments[1]->GetReturnType()) {
		throw BinderException("Cannot COMBINE aggregate states from different functions, %s <> %s",
		                      arguments[0]->GetReturnType().ToString(), arguments[1]->GetReturnType().ToString());
	}

	if (bound_function.GetName() == "finalize") {
		bound_function.SetReturnType(bind_data->aggr.GetReturnType());
	} else {
		D_ASSERT(bound_function.GetName() == "combine");
		bound_function.SetReturnType(arguments[0]->GetReturnType());
	}

	return std::move(bind_data);
}

void ExportAggregateFinalize(Vector &state, AggregateInputData &aggr_input_data, Vector &result, idx_t count,
                             idx_t offset) {
	D_ASSERT(offset == 0);
	const data_ptr_t *addresses_ptrs;
	if (state.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		if (count != 1) {
			throw InternalException("Finalize with a constant vector only supported with count of 1");
		}
		addresses_ptrs = ConstantVector::GetData<data_ptr_t>(state);
	} else {
		addresses_ptrs = FlatVector::GetData<data_ptr_t>(state);
	}

	auto layout = GetLayout(aggr_input_data.function);

	result.Flatten();
	if (layout.type.id() == LogicalTypeId::STRUCT) {
		SerializeStructFields(layout.type, layout.field, result, count, addresses_ptrs);
	} else if (layout.field.is_optional) {
		TemplateDispatch<StorePrimitiveOptionalOp>(layout.type.InternalType(), result, count, addresses_ptrs);
	} else {
		TemplateDispatch<StorePrimitiveOp>(layout.type.InternalType(), result, count, addresses_ptrs);
	}
}

unique_ptr<FunctionData> CombineAggrBind(BindAggregateFunctionInput &input) {
	auto &context = input.GetClientContext();
	auto &function = input.GetBoundFunction();
	auto &arguments = input.GetArguments();

	auto bind_data = BindAggregateStateInternal(context, function, arguments);

	// Copy underlying aggregate's callbacks into this function (same pattern as `ExportAggregateFunction::Bind`)
	function.SetStateSizeCallback(bind_data->aggr.GetStateSizeCallback());
	function.SetStateInitCallback(bind_data->aggr.GetStateInitCallback());
	function.SetStateCombineCallback(bind_data->aggr.GetStateCombineCallback());

	function.SetReturnType(arguments[0]->GetReturnType());

	return std::move(bind_data);
}

void CombineAggrUpdate(Vector inputs[], AggregateInputData &aggr_input_data, idx_t input_count, Vector &states,
                       idx_t count) {
	D_ASSERT(input_count == 1);

	auto &bind_data = aggr_input_data.bind_data->Cast<ExportAggregateBindData>();
	auto &underlying_aggr = bind_data.aggr;

	auto layout = GetLayout(underlying_aggr);

	UnifiedVectorFormat sdata;
	states.ToUnifiedFormat(sdata);
	auto state_ptrs = UnifiedVectorFormat::GetData<data_ptr_t>(sdata);

	inputs[0].Flatten();

	UnifiedVectorFormat input_data;
	inputs[0].ToUnifiedFormat(input_data);

	auto aligned_size = layout.total_state_size;
	unsafe_unique_array<data_t> temp_state_buf = make_unsafe_uniq_array<data_t>(count * aligned_size);

	// source_vec holds pointers to the binary states buffer (temp_state_buf) deserialized from the input states
	Vector source_vec(LogicalType::POINTER);
	auto source_data = FlatVector::Writer<data_ptr_t>(source_vec, count);

	// target_vec will hold pointers to the binary state buffer where the combined states should be stored, built by the
	// underlying aggregate function's combine callback
	Vector target_vec(LogicalType::POINTER);
	auto target_data = FlatVector::Writer<data_ptr_t>(target_vec, count);

	for (idx_t i = 0; i < count; i++) {
		auto temp_ptr = temp_state_buf.get() + i * aligned_size;
		underlying_aggr.GetStateInitCallback()(underlying_aggr, temp_ptr);
		source_data.WriteValue(temp_ptr);
		target_data.WriteValue(state_ptrs[sdata.sel->get_index(i)]);
	}

	if (layout.type.id() == LogicalTypeId::STRUCT) {
		DeserializeStructFields(layout.type, layout.field, layout.total_state_size, inputs[0], input_data, count,
		                        temp_state_buf.get());
	} else if (layout.field.is_optional) {
		TemplateDispatch<LoadPrimitiveOptionalOp>(layout.type.InternalType(), inputs[0], input_data, count,
		                                          temp_state_buf.get(), layout.total_state_size);
	} else {
		TemplateDispatch<LoadPrimitiveOp>(layout.type.InternalType(), inputs[0], input_data, count,
		                                  temp_state_buf.get(), layout.total_state_size);
	}

	ArenaAllocator allocator(Allocator::DefaultAllocator());
	AggregateInputData combine_input(bind_data.aggr, bind_data.bind_data.get(), allocator,
	                                 AggregateCombineType::ALLOW_DESTRUCTIVE);
	underlying_aggr.GetStateCombineCallback()(source_vec, target_vec, combine_input, count);
}

void CombineAggrFinalize(Vector &state, AggregateInputData &aggr_input_data, Vector &result, idx_t count,
                         idx_t offset) {
	D_ASSERT(offset == 0);
	auto &bind_data = aggr_input_data.bind_data->Cast<ExportAggregateBindData>();
	auto &underlying_aggr = bind_data.aggr;
	const data_ptr_t *addresses_ptrs;
	if (state.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		if (count != 1) {
			throw InternalException("Finalize with a constant vector only supported with count of 1");
		}
		addresses_ptrs = ConstantVector::GetData<data_ptr_t>(state);
	} else {
		addresses_ptrs = FlatVector::GetData<data_ptr_t>(state);
	}

	auto layout = GetLayout(underlying_aggr);

	result.Flatten();
	if (layout.type.id() == LogicalTypeId::STRUCT) {
		SerializeStructFields(layout.type, layout.field, result, count, addresses_ptrs);
	} else if (layout.field.is_optional) {
		TemplateDispatch<StorePrimitiveOptionalOp>(layout.type.InternalType(), result, count, addresses_ptrs);
	} else {
		TemplateDispatch<StorePrimitiveOp>(layout.type.InternalType(), result, count, addresses_ptrs);
	}
}

// constructs the AGGREGATE_STATE type for the given bound aggregate function
// the state layout (a struct) is aliased to AGGREGATE_STATE, with the function name and signature stored in the
// extension type info so that the aggregate can be re-bound later (e.g. by FINALIZE/COMBINE)
LogicalType CreateAggregateStateType(const BoundAggregateFunction &bound_function) {
	LogicalType state_layout = bound_function.GetStateType().type;
	state_layout.SetAlias("AGGREGATE_STATE");
	auto ext_info = make_uniq<ExtensionTypeInfo>();
	ext_info->properties.emplace("function_name", bound_function.GetName());
	vector<Value> arguments;
	for (auto &arg : bound_function.GetOriginalArguments().empty() ? bound_function.GetArguments()
	                                                               : bound_function.GetOriginalArguments()) {
		arguments.push_back(Value::TYPE(arg));
	}
	ext_info->properties.emplace("parameters", Value::LIST(LogicalType::TYPE(), std::move(arguments)));
	state_layout.SetExtensionInfo(std::move(ext_info));
	return state_layout;
}

unique_ptr<FunctionData> ToAggregateStateBind(BindScalarFunctionInput &input) {
	auto &bound_function = input.GetBoundFunction();
	auto &arguments = input.GetArguments();
	auto &context = input.GetClientContext();
	for (idx_t i = 1; i < 3; i++) {
		if (arguments[i]->HasParameter()) {
			throw ParameterNotResolvedException();
		}
		if (!arguments[i]->IsFoldable()) {
			throw BinderException("to_aggregate_state: the aggregate name and signature must be constant");
		}
	}
	auto function_name_val = ExpressionExecutor::EvaluateScalar(context, *arguments[1]);
	if (function_name_val.IsNull()) {
		throw BinderException("to_aggregate_state: the aggregate name cannot be NULL");
	}
	auto function_name = StringValue::Get(function_name_val);

	auto signature_val = ExpressionExecutor::EvaluateScalar(context, *arguments[2]);
	if (signature_val.IsNull()) {
		throw BinderException("to_aggregate_state: the signature must be a list of types");
	}
	vector<LogicalType> argument_types;
	for (auto &arg : ListValue::GetChildren(signature_val)) {
		if (arg.IsNull()) {
			throw BinderException("to_aggregate_state: the signature cannot contain NULL values");
		}
		if (arg.type().id() == LogicalTypeId::TYPE) {
			argument_types.push_back(TypeValue::GetType(arg));
		} else if (arg.type().id() == LogicalTypeId::VARCHAR) {
			argument_types.push_back(TransformStringToLogicalType(StringValue::Get(arg), context));
		} else {
			throw BinderException("to_aggregate_state: the signature must be a list of types");
		}
	}

	auto bind_data = BindExportedAggregate(context, function_name, argument_types);
	auto &aggr = bind_data->aggr;
	if (!aggr.HasGetStateTypeCallback()) {
		throw BinderException(
		    "Aggregate function \"%s\" does not have a state type callback defined - cannot convert to its state",
		    function_name);
	}
	auto state_layout = aggr.GetStateType().type;
	bound_function.GetArguments()[0] = state_layout;
	bound_function.SetReturnType(CreateAggregateStateType(aggr));
	return std::move(bind_data);
}

void ToAggregateStateFunction(DataChunk &input, ExpressionState &state, Vector &result) {
	// the input type is verified to match the state layout at bind time - we only need to reinterpret the vector
	result.Reinterpret(input.data[0]);
}

} // namespace

void ExportAggregateFunction::SetStateExport(BoundAggregateExpression &aggregate, LogicalType state_layout) {
	auto &bound_function = aggregate.FunctionMutable();
	bound_function.SetStateFinalizeCallback(ExportAggregateFinalize);
	// statistics propagation is no longer correct post
	bound_function.SetStatisticsCallback(nullptr);
	bound_function.SetReturnType(state_layout);
	// exported state always produces a valid (non-NULL) struct even for empty inputs
	bound_function.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	aggregate.StateExportModeMutable() = AggregateStateExportMode::STATE_EXPORT;
	aggregate.SetReturnType(std::move(state_layout));
}

unique_ptr<BoundAggregateExpression>
ExportAggregateFunction::Bind(unique_ptr<BoundAggregateExpression> child_aggregate) {
	auto &bound_function = child_aggregate->Function();
	if (!bound_function.HasStateCombineCallback()) {
		throw BinderException("Cannot use EXPORT_STATE for non-combinable function %s", bound_function.GetName());
	}
	if (bound_function.HasStateDestructorCallback()) {
		throw BinderException("Cannot use EXPORT_STATE on aggregate functions with custom destructors");
	}
	// this should be required
	D_ASSERT(bound_function.HasStateSizeCallback());
	D_ASSERT(bound_function.HasStateFinalizeCallback());

	D_ASSERT(child_aggregate->Function().GetReturnType().id() != LogicalTypeId::INVALID);
	if (!bound_function.HasGetStateTypeCallback()) {
		throw NotImplementedException(
		    "Aggregate function \"%s\" does not have a state type callback defined - cannot export state",
		    bound_function.GetName());
	}
	SetStateExport(*child_aggregate, CreateAggregateStateType(bound_function));
	return child_aggregate;
}

ExportAggregateFunctionBindData::ExportAggregateFunctionBindData(unique_ptr<Expression> aggregate_p) {
	D_ASSERT(aggregate_p->GetExpressionType() == ExpressionType::BOUND_AGGREGATE);
	aggregate = unique_ptr_cast<Expression, BoundAggregateExpression>(std::move(aggregate_p));
}

unique_ptr<FunctionData> ExportAggregateFunctionBindData::Copy() const {
	return make_uniq<ExportAggregateFunctionBindData>(aggregate->Copy());
}

bool ExportAggregateFunctionBindData::Equals(const FunctionData &other_p) const {
	auto &other = other_p.Cast<ExportAggregateFunctionBindData>();
	return aggregate->Equals(*other.aggregate);
}

ScalarFunction FinalizeFun::GetFunction() {
	auto function = ScalarFunction("finalize", {LogicalTypeId::ANY}, LogicalTypeId::INVALID, AggregateStateFinalize,
	                               BindAggregateState, nullptr, InitFinalizeState);
	function.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);

	return function;
}

ScalarFunction CombineFun::GetFunction() {
	auto function = ScalarFunction("combine", {LogicalTypeId::ANY, LogicalTypeId::ANY}, LogicalTypeId::ANY,
	                               AggregateStateCombine, BindAggregateState, nullptr, InitCombineState);
	function.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	return function;
}

ScalarFunction ToAggregateStateFun::GetFunction() {
	auto function = ScalarFunction("to_aggregate_state",
	                               {LogicalTypeId::ANY, LogicalType::VARCHAR, LogicalType::LIST(LogicalType::ANY)},
	                               LogicalTypeId::ANY, ToAggregateStateFunction, ToAggregateStateBind);
	function.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	return function;
}

AggregateFunction CombineAggrFun::GetFunction() {
	auto function =
	    AggregateFunction("combine_aggr", {LogicalTypeId::ANY}, LogicalTypeId::ANY, nullptr, nullptr, CombineAggrUpdate,
	                      nullptr, CombineAggrFinalize, FunctionNullHandling::SPECIAL_HANDLING, nullptr,
	                      CombineAggrBind, nullptr, nullptr, nullptr);
	function.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	return function;
}

} // namespace duckdb
