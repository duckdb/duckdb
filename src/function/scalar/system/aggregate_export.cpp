#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/function/scalar/generic_common.hpp"
#include "duckdb/function/scalar/system_functions.hpp"
#include "duckdb/function/scalar/generic_functions.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

namespace duckdb {

namespace {

// aggregate state export
struct ExportAggregateBindData : public FunctionData {
	AggregateFunction aggr;
	idx_t state_size;

	explicit ExportAggregateBindData(AggregateFunction aggr_p, idx_t state_size_p)
	    : aggr(std::move(aggr_p)), state_size(state_size_p) {
	}

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<ExportAggregateBindData>(aggr, state_size);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<ExportAggregateBindData>();
		return aggr == other.aggr && state_size == other.state_size;
	}

	static ExportAggregateBindData &GetFrom(ExpressionState &state) {
		auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
		return func_expr.bind_info->Cast<ExportAggregateBindData>();
	}
};

template <class OP, class... ARGS>
static void TemplateDispatch(PhysicalType type, ARGS &&... args) {
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
		throw InternalException("Unsupported physical type: %s for aggregate state", TypeIdToString(type));
	}
}

struct AggregateStateLayout {
	AggregateStateLayout(const LogicalType &type, idx_t state_size)
	    : state_size(state_size), aligned_state_size(AlignValue(state_size)) {
		owned_type = type;
		is_struct = type.InternalType() == PhysicalType::STRUCT;
		if (is_struct) {
			child_types = &StructType::GetChildTypes(owned_type);
		}
	}

	// Reconstruct a packed binary state from the vector representation
	// Works only on a legacy state format where the entire state is stored as a blob
	void Load(Vector &vec, const UnifiedVectorFormat &state_data, idx_t row, data_ptr_t dest) {
		D_ASSERT(!is_struct);
		auto idx = state_data.sel->get_index(row);
		auto &blob = UnifiedVectorFormat::GetData<string_t>(state_data)[idx];
		if (blob.GetSize() != state_size) {
			throw IOException("Aggregate state size mismatch, expect %llu, got %llu", state_size, blob.GetSize());
		}
		memcpy(dest, blob.GetData(), state_size);
	}

	// Serializes a packed binary state back into a `Vector` format
	// Works only on a legacy state format where the entire state is stored as a blob
	void Store(Vector &result, idx_t row, data_ptr_t src) const {
		D_ASSERT(!is_struct);
		auto result_ptr = FlatVector::GetData<string_t>(result);
		result_ptr[row] = StringVector::AddStringOrBlob(result, const_char_ptr_cast(src), state_size);
	}

	bool is_struct;
	idx_t state_size;
	idx_t aligned_state_size;
	LogicalType owned_type;
	const child_list_t<LogicalType> *child_types = nullptr;
};

/*
 * Load a specific field from the struct aggregate state into the packed binary representation
 * By iterating over rows in the inner loop for a specific type T, the compiler is given a tight loop with a
 * predictable memory access pattern. Since the field in a struct is a contiguous array of type `T`, the compiler
 * can easily vectorize the read operations using SIMD instructions
 */
struct LoadFieldOp {
	template <class T>
	static void Operation(const AggregateStateLayout &layout, Vector &struct_vec, idx_t field_idx,
	                      const UnifiedVectorFormat &state_data, idx_t count, data_ptr_t base_ptr, idx_t field_offset) {
		auto &child = *StructVector::GetEntries(struct_vec)[field_idx];
		auto child_data = FlatVector::GetData<T>(child);

		for (idx_t row = 0; row < count; row++) {
			auto row_idx = state_data.sel->get_index(row);
			if (!state_data.validity.RowIsValid(row_idx)) {
				continue;
			}

			auto dest = base_ptr + row * layout.aligned_state_size + field_offset;
			*reinterpret_cast<T *>(dest) = child_data[row];
		}
	}
};

struct StoreFieldOp {
	template <class T>
	static void Operation(Vector &struct_vec, idx_t field_idx, idx_t count, data_ptr_t *sources, idx_t field_offset) {
		auto &child = *StructVector::GetEntries(struct_vec)[field_idx];
		auto child_data = FlatVector::GetData<T>(child);

		for (idx_t row = 0; row < count; row++) {
			auto src = sources[row] + field_offset;
			child_data[row] = *reinterpret_cast<T *>(src);
		}
	}
};

struct CopyFromInputFieldOp {
	template <class T>
	static void Operation(Vector &input_vec, Vector &result_vec, idx_t field_idx, const SelectionVector &sel,
	                      idx_t count, const UnifiedVectorFormat &input_data) {
		auto &input_child = *StructVector::GetEntries(input_vec)[field_idx];
		auto input_child_data = FlatVector::GetData<T>(input_child);

		auto &result_child = *StructVector::GetEntries(result_vec)[field_idx];
		auto result_child_data = FlatVector::GetData<T>(result_child);

		for (idx_t i = 0; i < count; i++) {
			idx_t row = sel.get_index(i);
			auto src_idx = input_data.sel->get_index(row);
			result_child_data[row] = input_child_data[src_idx];
		}
	}
};

struct LoadFieldForSelectedRowsOp {
	template <class T>
	static void Operation(const AggregateStateLayout &layout, Vector &struct_vec, idx_t field_idx,
	                      const SelectionVector &sel, idx_t count, const UnifiedVectorFormat &state_data,
	                      data_ptr_t base_ptr, idx_t field_offset) {
		auto &child = *StructVector::GetEntries(struct_vec)[field_idx];
		auto child_data = FlatVector::GetData<T>(child);

		for (idx_t i = 0; i < count; i++) {
			idx_t row = sel.get_index(i);
			auto src_idx = state_data.sel->get_index(row);
			auto dest = base_ptr + i * layout.aligned_state_size + field_offset;
			*reinterpret_cast<T *>(dest) = child_data[src_idx];
		}
	}
};

struct StoreFieldForSelectedRowsOp {
	template <class T>
	static void Operation(const AggregateStateLayout &layout, Vector &result, idx_t field_idx,
	                      const SelectionVector &sel, idx_t count, data_ptr_t base_ptr, idx_t field_offset) {
		auto &child = *StructVector::GetEntries(result)[field_idx];
		auto child_data = FlatVector::GetData<T>(child);

		for (idx_t i = 0; i < count; i++) {
			idx_t row = sel.get_index(i);
			auto src = base_ptr + i * layout.aligned_state_size + field_offset;
			child_data[row] = *reinterpret_cast<T *>(src);
		}
	}
};

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
	D_ASSERT(input.data[0].GetType().id() == LogicalTypeId::LEGACY_AGGREGATE_STATE ||
	         input.data[0].GetType().id() == LogicalTypeId::AGGREGATE_STATE);

	AggregateStateLayout layout(input.data[0].GetType(), bind_data.state_size);

	auto state_vec_ptr = FlatVector::GetData<data_ptr_t>(local_state.addresses);

	input.data[0].Flatten(input.size());

	UnifiedVectorFormat state_data;
	input.data[0].ToUnifiedFormat(input.size(), state_data);

	if (layout.is_struct) {
		for (idx_t i = 0; i < input.size(); i++) {
			state_vec_ptr[i] = local_state.state_buffer.get() + i * layout.aligned_state_size;
		}

		idx_t offset = 0;
		for (idx_t f = 0; f < layout.child_types->size(); f++) {
			auto type = layout.child_types->at(f).second;
			idx_t field_size = GetTypeIdSize(type.InternalType());
			idx_t alignment = MinValue<idx_t>(field_size, 8);
			offset = AlignValue(offset, alignment);

			auto &struct_vector = input.data[0];

			// switch with templating since we want the compiler to optimize this when copying (reinterpret_cast)
			// will internally use SIMD
			auto physical = type.InternalType();
			TemplateDispatch<LoadFieldOp>(physical, layout, struct_vector, f, state_data, input.size(),
			                              local_state.state_buffer.get(), offset);

			offset += field_size;
		}
	} else {
		for (idx_t i = 0; i < input.size(); i++) {
			auto target_ptr = local_state.state_buffer.get() + layout.aligned_state_size * i;
			auto state_idx = state_data.sel->get_index(i);

			if (state_data.validity.RowIsValid(state_idx)) {
				layout.Load(input.data[0], state_data, i, target_ptr);
			} else {
				// create a dummy state because finalize does not understand NULLs in its input
				// we put the NULL back in explicitly below
				bind_data.aggr.GetStateInitCallback()(bind_data.aggr, data_ptr_cast(target_ptr));
			}
			state_vec_ptr[i] = data_ptr_cast(target_ptr);
		}
	}

	AggregateInputData aggr_input_data(nullptr, local_state.allocator);
	bind_data.aggr.GetStateFinalizeCallback()(local_state.addresses, aggr_input_data, result, input.size(), 0);

	for (idx_t i = 0; i < input.size(); i++) {
		auto state_idx = state_data.sel->get_index(i);
		if (!state_data.validity.RowIsValid(state_idx)) {
			FlatVector::SetNull(result, i, true);
		}
	}
}

void AggregateStateCombine(DataChunk &input, ExpressionState &state_p, Vector &result) {
	auto &bind_data = ExportAggregateBindData::GetFrom(state_p);
	auto &local_state = ExecuteFunctionState::GetFunctionState(state_p)->Cast<CombineState>();
	local_state.allocator.Reset();

	D_ASSERT(bind_data.state_size == bind_data.aggr.GetStateSizeCallback()(bind_data.aggr));

	D_ASSERT(input.data.size() == 2);
	D_ASSERT(input.data[0].GetType().id() == LogicalTypeId::LEGACY_AGGREGATE_STATE ||
	         input.data[0].GetType().id() == LogicalTypeId::AGGREGATE_STATE);
	D_ASSERT(input.data[0].GetType() == result.GetType());

	AggregateStateLayout layout(input.data[0].GetType(), bind_data.state_size);

	if (input.data[0].GetType().InternalType() != input.data[1].GetType().InternalType()) {
		throw IOException("Aggregate state combine type mismatch, expect %s, got %s",
		                  input.data[0].GetType().ToString(), input.data[1].GetType().ToString());
	}

	if (layout.is_struct) {
		input.data[0].Flatten(input.size());
		input.data[1].Flatten(input.size());
		result.Flatten(input.size());
	}

	UnifiedVectorFormat state0_data, state1_data;
	input.data[0].ToUnifiedFormat(input.size(), state0_data);
	input.data[1].ToUnifiedFormat(input.size(), state1_data);

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
		if (layout.is_struct) {
			idx_t offset_in_state = 0;
			for (idx_t field_idx = 0; field_idx < layout.child_types->size(); field_idx++) {
				auto &field_type = layout.child_types->at(field_idx).second;
				auto physical = field_type.InternalType();
				idx_t field_size = GetTypeIdSize(physical);
				idx_t alignment = MinValue<idx_t>(field_size, 8);
				offset_in_state = AlignValue(offset_in_state, alignment);
				TemplateDispatch<CopyFromInputFieldOp>(physical, input.data[0], result, field_idx, copy_from_0_sel,
				                                       copy_from_0_count, state0_data);
				offset_in_state += field_size;
			}
		} else {
			for (idx_t i = 0; i < copy_from_0_count; i++) {
				idx_t row = copy_from_0_sel.get_index(i);
				layout.Load(input.data[0], state0_data, row, local_state.state_buffer0.get());
				layout.Store(result, row, local_state.state_buffer0.get());
			}
		}
	}
	// copy_from_1: input0 is null, copy input1
	if (copy_from_1_count > 0) {
		if (layout.is_struct) {
			idx_t offset_in_state = 0;
			for (idx_t field_idx = 0; field_idx < layout.child_types->size(); field_idx++) {
				auto &field_type = layout.child_types->at(field_idx).second;
				auto physical = field_type.InternalType();
				idx_t field_size = GetTypeIdSize(physical);
				idx_t alignment = MinValue<idx_t>(field_size, 8);
				offset_in_state = AlignValue(offset_in_state, alignment);
				TemplateDispatch<CopyFromInputFieldOp>(physical, input.data[1], result, field_idx, copy_from_1_sel,
				                                       copy_from_1_count, state1_data);
				offset_in_state += field_size;
			}
		} else {
			for (idx_t i = 0; i < copy_from_1_count; i++) {
				idx_t row = copy_from_1_sel.get_index(i);
				layout.Load(input.data[1], state1_data, row, local_state.state_buffer1.get());
				layout.Store(result, row, local_state.state_buffer1.get());
			}
		}
	}

	// Handle both-valid rows - batched load, combine, store
	if (both_valid_count > 0) {
		auto state0_ptrs = FlatVector::GetData<data_ptr_t>(local_state.addresses0);
		auto state1_ptrs = FlatVector::GetData<data_ptr_t>(local_state.addresses1);

		// Pack state buffer pointers in selection order (not row order)
		for (idx_t i = 0; i < both_valid_count; i++) {
			state0_ptrs[i] = local_state.state_buffer0.get() + i * layout.aligned_state_size;
			state1_ptrs[i] = local_state.state_buffer1.get() + i * layout.aligned_state_size;
		}

		if (layout.is_struct) {
			// Use tight loops to load both inputs
			idx_t offset_in_state = 0;
			for (idx_t field_idx = 0; field_idx < layout.child_types->size(); field_idx++) {
				auto &field_type = layout.child_types->at(field_idx).second;
				auto physical = field_type.InternalType();
				idx_t field_size = GetTypeIdSize(physical);
				idx_t alignment = MinValue<idx_t>(field_size, 8);
				offset_in_state = AlignValue(offset_in_state, alignment);

				TemplateDispatch<LoadFieldForSelectedRowsOp>(physical, layout, input.data[0], field_idx, both_valid_sel,
				                                             both_valid_count, state0_data,
				                                             local_state.state_buffer0.get(), offset_in_state);
				TemplateDispatch<LoadFieldForSelectedRowsOp>(physical, layout, input.data[1], field_idx, both_valid_sel,
				                                             both_valid_count, state1_data,
				                                             local_state.state_buffer1.get(), offset_in_state);
				offset_in_state += field_size;
			}
		} else {
			// Handle blob case - load both inputs for all both_valid rows
			for (idx_t i = 0; i < both_valid_count; i++) {
				idx_t row = both_valid_sel.get_index(i);
				layout.Load(input.data[0], state0_data, row, state0_ptrs[i]);
				layout.Load(input.data[1], state1_data, row, state1_ptrs[i]);
			}
		}

		// Single batched combine call
		AggregateInputData aggr_input_data(nullptr, local_state.allocator, AggregateCombineType::ALLOW_DESTRUCTIVE);
		bind_data.aggr.GetStateCombineCallback()(local_state.addresses0, local_state.addresses1, aggr_input_data,
		                                         both_valid_count);

		// Store results
		if (layout.is_struct) {
			idx_t offset_in_state = 0;
			for (idx_t field_idx = 0; field_idx < layout.child_types->size(); field_idx++) {
				auto &field_type = layout.child_types->at(field_idx).second;
				auto physical = field_type.InternalType();
				idx_t field_size = GetTypeIdSize(physical);
				idx_t alignment = MinValue<idx_t>(field_size, 8);
				offset_in_state = AlignValue(offset_in_state, alignment);

				TemplateDispatch<StoreFieldForSelectedRowsOp>(physical, layout, result, field_idx, both_valid_sel,
				                                              both_valid_count, local_state.state_buffer1.get(),
				                                              offset_in_state);
				offset_in_state += field_size;
			}
		} else {
			// Handle blob case - store results using the legacy not tight-loop strategy
			for (idx_t i = 0; i < both_valid_count; i++) {
				idx_t row = both_valid_sel.get_index(i);
				layout.Store(result, row, state1_ptrs[i]);
			}
		}
	}
}

unique_ptr<FunctionData> BindAggregateState(ClientContext &context, ScalarFunction &bound_function,
                                            vector<unique_ptr<Expression>> &arguments) {
	// grab the aggregate type and bind the aggregate again

	// the aggregate name and types are in the logical type of the aggregate state, make sure its sane
	auto &arg_return_type = arguments[0]->return_type;
	for (auto &arg_type : bound_function.arguments) {
		arg_type = arg_return_type;
	}

	if (arg_return_type.id() != LogicalTypeId::LEGACY_AGGREGATE_STATE &&
	    arg_return_type.id() != LogicalTypeId::AGGREGATE_STATE) {
		throw BinderException("Can only FINALIZE aggregate state, not %s", arg_return_type.ToString());
	}
	// combine
	if (arguments.size() == 2 && arguments[0]->return_type != arguments[1]->return_type &&
	    arguments[1]->return_type.id() != LogicalTypeId::BLOB) {
		throw BinderException("Cannot COMBINE aggregate states from different functions, %s <> %s",
		                      arguments[0]->return_type.ToString(), arguments[1]->return_type.ToString());
	}

	// following error states are only reachable when someone messes up creating the state_type
	// which is impossible from SQL

	auto state_type = AggregateStateType::GetStateType(arg_return_type);

	// now we can look up the function in the catalog again and bind it
	auto &func = Catalog::GetSystemCatalog(context).GetEntry<AggregateFunctionCatalogEntry>(context, DEFAULT_SCHEMA,
	                                                                                        state_type.function_name);
	if (func.type != CatalogType::AGGREGATE_FUNCTION_ENTRY) {
		throw InternalException("Could not find aggregate %s", state_type.function_name);
	}
	auto &aggr = func.Cast<AggregateFunctionCatalogEntry>();

	ErrorData error;

	FunctionBinder function_binder(context);
	auto best_function =
	    function_binder.BindFunction(aggr.name, aggr.functions, state_type.bound_argument_types, error);
	if (!best_function.IsValid()) {
		throw InternalException("Could not re-bind exported aggregate %s: %s", state_type.function_name,
		                        error.Message());
	}
	auto bound_aggr = aggr.functions.GetFunctionByOffset(best_function.GetIndex());
	if (bound_aggr.GetBindCallback()) {
		// FIXME: this is really hacky
		// but the aggregate state export needs a rework around how it handles more complex aggregates anyway
		vector<unique_ptr<Expression>> args;
		args.reserve(state_type.bound_argument_types.size());
		for (auto &arg_type : state_type.bound_argument_types) {
			args.push_back(make_uniq<BoundConstantExpression>(Value(arg_type)));
		}
		auto bind_info = bound_aggr.GetBindCallback()(context, bound_aggr, args);
		if (bind_info) {
			throw BinderException("Aggregate function with bind info not supported yet in aggregate state export");
		}
	}

	if (bound_aggr.GetReturnType() != state_type.return_type ||
	    bound_aggr.arguments != state_type.bound_argument_types) {
		throw InternalException("Type mismatch for exported aggregate %s", state_type.function_name);
	}

	if (bound_function.name == "finalize") {
		bound_function.SetReturnType(bound_aggr.GetReturnType());
	} else {
		D_ASSERT(bound_function.name == "combine");
		bound_function.SetReturnType(arg_return_type);
	}

	return make_uniq<ExportAggregateBindData>(bound_aggr, bound_aggr.GetStateSizeCallback()(bound_aggr));
}

void ExportAggregateFinalize(Vector &state, AggregateInputData &aggr_input_data, Vector &result, idx_t count,
                             idx_t offset) {
	D_ASSERT(offset == 0);
	auto &bind_data = aggr_input_data.bind_data->Cast<ExportAggregateFunctionBindData>();
	auto addresses_ptrs = FlatVector::GetData<data_ptr_t>(state);

	auto state_size = bind_data.aggregate->function.GetStateSizeCallback()(bind_data.aggregate->function);

	// Note: The underlying state type should always be a struct (we have a D_ASSERT for that in `GetStateType`
	bool should_result_as_struct = bind_data.aggregate->function.HasGetStateTypeCallback();
	if (should_result_as_struct) {
		AggregateStateLayout layout(bind_data.aggregate->function.GetStateType(), state_size);

		result.Flatten(count);
		idx_t offset_in_state = 0;
		for (idx_t field_idx = 0; field_idx < layout.child_types->size(); field_idx++) {
			auto &field_type = layout.child_types->at(field_idx).second;
			auto physical = field_type.InternalType();
			idx_t field_size = GetTypeIdSize(physical);
			idx_t alignment = MinValue<idx_t>(field_size, 8);
			offset_in_state = AlignValue(offset_in_state, alignment);

			TemplateDispatch<StoreFieldOp>(physical, result, field_idx, count, addresses_ptrs, offset_in_state);

			offset_in_state += field_size;
		}
		return;
	}

	auto blob_ptr = FlatVector::GetData<string_t>(result);
	for (idx_t row_idx = 0; row_idx < count; row_idx++) {
		auto data_ptr = addresses_ptrs[row_idx];
		blob_ptr[row_idx] = StringVector::AddStringOrBlob(result, const_char_ptr_cast(data_ptr), state_size);
	}
}

void ExportStateAggregateSerialize(Serializer &serializer, const optional_ptr<FunctionData> bind_data_p,
                                   const AggregateFunction &function) {
	throw NotImplementedException("FIXME: export state serialize");
}

unique_ptr<FunctionData> ExportStateAggregateDeserialize(Deserializer &deserializer, AggregateFunction &function) {
	throw NotImplementedException("FIXME: export state deserialize");
}

void ExportStateScalarSerialize(Serializer &serializer, const optional_ptr<FunctionData> bind_data_p,
                                const ScalarFunction &function) {
	throw NotImplementedException("FIXME: export state serialize");
}

unique_ptr<FunctionData> ExportStateScalarDeserialize(Deserializer &deserializer, ScalarFunction &function) {
	throw NotImplementedException("FIXME: export state deserialize");
}

} // namespace

unique_ptr<BoundAggregateExpression>
ExportAggregateFunction::Bind(unique_ptr<BoundAggregateExpression> child_aggregate) {
	auto &bound_function = child_aggregate->function;
	if (!bound_function.HasStateCombineCallback()) {
		throw BinderException("Cannot use EXPORT_STATE for non-combinable function %s", bound_function.name);
	}
	if (bound_function.HasBindCallback()) {
		throw BinderException("Cannot use EXPORT_STATE on aggregate functions with custom binders");
	}
	if (bound_function.HasStateDestructorCallback()) {
		throw BinderException("Cannot use EXPORT_STATE on aggregate functions with custom destructors");
	}
	// this should be required
	D_ASSERT(bound_function.HasStateSizeCallback());
	D_ASSERT(bound_function.HasStateFinalizeCallback());

	D_ASSERT(child_aggregate->function.GetReturnType().id() != LogicalTypeId::INVALID);
#ifdef DEBUG
	for (auto &arg_type : child_aggregate->function.arguments) {
		D_ASSERT(arg_type.id() != LogicalTypeId::INVALID);
	}
#endif
	auto export_bind_data = make_uniq<ExportAggregateFunctionBindData>(child_aggregate->Copy());
	aggregate_state_t state_type(child_aggregate->function.name, child_aggregate->function.GetReturnType(),
	                             child_aggregate->function.arguments);

	LogicalType return_type;
	if (bound_function.HasGetStateTypeCallback()) {
		LogicalType state_layout = bound_function.GetStateType();
		auto struct_child_types = StructType::GetChildTypes(state_layout);
		return_type = LogicalType::AGGREGATE_STATE(std::move(state_type), std::move(struct_child_types));
	} else {
		return_type = LogicalType::LEGACY_AGGREGATE_STATE(std::move(state_type));
	}

	auto export_function =
	    AggregateFunction("aggregate_state_export_" + bound_function.name, bound_function.arguments, return_type,
	                      bound_function.GetStateSizeCallback(), bound_function.GetStateInitCallback(),
	                      bound_function.GetStateUpdateCallback(), bound_function.GetStateCombineCallback(),
	                      ExportAggregateFinalize, bound_function.GetStateSimpleUpdateCallback(),
	                      /* can't bind this again */ nullptr, /* no dynamic state yet */ nullptr,
	                      /* can't propagate statistics */ nullptr, nullptr);
	export_function.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	export_function.SetSerializeCallback(ExportStateAggregateSerialize);
	export_function.SetDeserializeCallback(ExportStateAggregateDeserialize);

	return make_uniq<BoundAggregateExpression>(export_function, std::move(child_aggregate->children),
	                                           std::move(child_aggregate->filter), std::move(export_bind_data),
	                                           child_aggregate->aggr_type);
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

ScalarFunction CreateFinalizeFun(LogicalTypeId aggregate_state_logical_type_id) {
	auto function = ScalarFunction("finalize", {aggregate_state_logical_type_id}, LogicalTypeId::INVALID,
	                               AggregateStateFinalize, BindAggregateState, nullptr, nullptr, InitFinalizeState);
	function.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	function.SetSerializeCallback(ExportStateScalarSerialize);
	function.SetDeserializeCallback(ExportStateScalarDeserialize);

	return function;
}

ScalarFunction CreateCombineFun(LogicalTypeId aggregate_state_logical_type_id) {
	auto function = ScalarFunction("combine", {aggregate_state_logical_type_id, LogicalTypeId::ANY},
	                               aggregate_state_logical_type_id, AggregateStateCombine, BindAggregateState, nullptr,
	                               nullptr, InitCombineState);
	function.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	function.SetSerializeCallback(ExportStateScalarSerialize);
	function.SetDeserializeCallback(ExportStateScalarDeserialize);
	return function;
}

ScalarFunctionSet FinalizeFun::GetFunctions() {
	ScalarFunctionSet finalize_set;

	auto blob_finalize = CreateFinalizeFun(LogicalTypeId::LEGACY_AGGREGATE_STATE);
	finalize_set.AddFunction(blob_finalize);

	auto struct_based_finalize = CreateFinalizeFun(LogicalTypeId::AGGREGATE_STATE);
	finalize_set.AddFunction(struct_based_finalize);

	return finalize_set;
}

ScalarFunctionSet CombineFun::GetFunctions() {
	ScalarFunctionSet combine_set;

	auto blob_combine = CreateCombineFun(LogicalTypeId::LEGACY_AGGREGATE_STATE);
	combine_set.AddFunction(blob_combine);

	auto struct_based_combine = CreateCombineFun(LogicalTypeId::AGGREGATE_STATE);
	combine_set.AddFunction(struct_based_combine);

	return combine_set;
}

} // namespace duckdb
