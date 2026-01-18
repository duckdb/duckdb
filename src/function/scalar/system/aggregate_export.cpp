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

struct AggregateStateLayout {
	AggregateStateLayout(const LogicalType &type, idx_t state_size) : state_size(state_size) {
		auto &info = AggregateStateType::GetStateType(type);
		is_struct = info.state_type.id() == LogicalTypeId::STRUCT;
		if (is_struct) {
			child_types = &StructType::GetChildTypes(info.state_type);
		}
	}

	// Reconstruct a packed binary state from the vector representation
	void Load(Vector &vec, const UnifiedVectorFormat &state_data, idx_t row, data_ptr_t dest) {
		if (!is_struct) {
			auto idx = state_data.sel->get_index(row);
			auto &blob = UnifiedVectorFormat::GetData<string_t>(state_data)[idx];
			if (blob.GetSize() != state_size) {
				throw IOException("Aggregate state size mismatch, expect %llu, got %llu", state_size, blob.GetSize());
			}
			memcpy(dest, blob.GetData(), state_size);
		} else {
			auto &children = StructVector::GetEntries(vec);
			D_ASSERT(child_types->size() == children.size());
			idx_t offset = 0;
			for (idx_t f = 0; f < child_types->size(); f++) {
				auto &field_type = (*child_types)[f].second;
				auto physical = field_type.InternalType();
				auto field_size = GetTypeIdSize(physical);
				idx_t alignment = MinValue<idx_t>(field_size, 8);
				offset = AlignValue(offset, alignment);

				auto &child = *children[f];
				auto data = data_ptr_cast(FlatVector::GetData(child));
				memcpy(dest + offset, data + row * field_size, field_size);
				offset += field_size;
			}
		}
	}

	// Serializes a packed binary state back into a `Vector` format
	void Store(Vector &result, idx_t row, data_ptr_t src) const {
		if (!is_struct) {
			auto result_ptr = FlatVector::GetData<string_t>(result);
			result_ptr[row] = StringVector::AddStringOrBlob(result, const_char_ptr_cast(src), state_size);
		} else {
			auto &children = StructVector::GetEntries(result);
			D_ASSERT(child_types->size() == children.size());
			idx_t offset = 0;
			for (idx_t f = 0; f < child_types->size(); f++) {
				auto &field_type = (*child_types)[f].second;
				auto physical = field_type.InternalType();
				auto field_size = GetTypeIdSize(physical);
				idx_t alignment = MinValue<idx_t>(field_size, 8);
				offset = AlignValue(offset, alignment);

				auto &child = *children[f];
				auto data = data_ptr_cast(FlatVector::GetData(child));
				memcpy(data + row * field_size, src + offset, field_size);
				offset += field_size;
			}
		}
	}

	bool is_struct;
	idx_t state_size;
	const child_list_t<LogicalType> *child_types = nullptr;
};

struct CombineState : public FunctionLocalState {
	idx_t state_size;

	unsafe_unique_array<data_t> state_buffer0, state_buffer1;
	Vector state_vector0, state_vector1;

	ArenaAllocator allocator;

	explicit CombineState(idx_t state_size_p)
	    : state_size(state_size_p), state_buffer0(make_unsafe_uniq_array<data_t>(state_size_p)),
	      state_buffer1(make_unsafe_uniq_array<data_t>(state_size_p)),
	      state_vector0(Value::POINTER(CastPointerToValue(state_buffer0.get()))),
	      state_vector1(Value::POINTER(CastPointerToValue(state_buffer1.get()))),
	      allocator(Allocator::DefaultAllocator()) {
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
	D_ASSERT(input.data[0].GetType().id() == LogicalTypeId::AGGREGATE_STATE);

	AggregateStateLayout layout(input.data[0].GetType(), bind_data.state_size);

	const auto aligned_state_size = AlignValue(bind_data.state_size);
	auto state_vec_ptr = FlatVector::GetData<data_ptr_t>(local_state.addresses);

	if (layout.is_struct) {
		input.data[0].Flatten(input.size());
	}

	UnifiedVectorFormat state_data;
	input.data[0].ToUnifiedFormat(input.size(), state_data);

	for (idx_t i = 0; i < input.size(); i++) {
		auto target_ptr = local_state.state_buffer.get() + aligned_state_size * i;
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
	D_ASSERT(input.data[0].GetType().id() == LogicalTypeId::AGGREGATE_STATE);
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

	for (idx_t i = 0; i < input.size(); i++) {
		const bool is_null0 = !state0_data.validity.RowIsValid(state0_data.sel->get_index(i));
		const bool is_null1 = !state1_data.validity.RowIsValid(state1_data.sel->get_index(i));

		if (is_null0 && is_null1) {
			FlatVector::SetNull(result, i, true);
			continue;
		}

		if (is_null0 || is_null1) {
			auto &non_null_vec = is_null0 ? input.data[1] : input.data[0];
			auto &non_null_state_data = is_null0 ? state1_data : state0_data;

			layout.Load(non_null_vec, non_null_state_data, i, local_state.state_buffer1.get());
			layout.Store(result, i, local_state.state_buffer1.get());
			continue;
		}

		// Both are non-NULL, combine them
		layout.Load(input.data[0], state0_data, i, local_state.state_buffer0.get());
		layout.Load(input.data[1], state1_data, i, local_state.state_buffer1.get());

		AggregateInputData aggr_input_data(nullptr, local_state.allocator, AggregateCombineType::ALLOW_DESTRUCTIVE);
		bind_data.aggr.GetStateCombineCallback()(local_state.state_vector0, local_state.state_vector1, aggr_input_data,
		                                         1);

		layout.Store(result, i, local_state.state_buffer1.get());
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

	if (arg_return_type.id() != LogicalTypeId::AGGREGATE_STATE) {
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
	auto state_ptrs = FlatVector::GetData<data_ptr_t>(state);

	auto aggregate_state_type = bind_data.aggregate->function.GetStateType();
	if (aggregate_state_type.id() == LogicalTypeId::STRUCT) {
		result.Flatten(count);

		auto &children = StructVector::GetEntries(result);
		auto &struct_fields = StructType::GetChildTypes(aggregate_state_type);

		idx_t offset_in_state = 0;
		for (idx_t field_idx = 0; field_idx < struct_fields.size(); field_idx++) {
			auto &field_type = struct_fields[field_idx].second;
			auto physical = field_type.InternalType();
			auto field_size = GetTypeIdSize(physical);

			idx_t alignment = MinValue<idx_t>(field_size, 8);
			offset_in_state = AlignValue(offset_in_state, alignment);

			auto &child_vector = *children[field_idx];
			child_vector.Flatten(count);

			auto data = FlatVector::GetData(child_vector);
			for (idx_t row = 0; row < count; row++) {
				auto src = state_ptrs[row] + offset_in_state;
				memcpy(data + row * field_size, src, field_size);
			}

			offset_in_state += field_size;
		}
		return;
	}

	auto state_size = bind_data.aggregate->function.GetStateSizeCallback()(bind_data.aggregate->function);
	auto blob_ptr = FlatVector::GetData<string_t>(result);
	for (idx_t row_idx = 0; row_idx < count; row_idx++) {
		auto data_ptr = state_ptrs[row_idx];
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
	LogicalType state_layout = LogicalType::INVALID;

	if (bound_function.HasGetStateTypeCallback()) {
		state_layout = bound_function.GetStateType();
	}
	aggregate_state_t state_type(child_aggregate->function.name, child_aggregate->function.GetReturnType(),
	                             child_aggregate->function.arguments, state_layout);

	const auto return_type = LogicalType::AGGREGATE_STATE(std::move(state_type));

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

ScalarFunction FinalizeFun::GetFunction() {
	auto result = ScalarFunction("finalize", {LogicalTypeId::AGGREGATE_STATE}, LogicalTypeId::INVALID,
	                             AggregateStateFinalize, BindAggregateState, nullptr, nullptr, InitFinalizeState);
	result.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	result.SetSerializeCallback(ExportStateScalarSerialize);
	result.SetDeserializeCallback(ExportStateScalarDeserialize);
	return result;
}

ScalarFunction CombineFun::GetFunction() {
	auto result =
	    ScalarFunction("combine", {LogicalTypeId::AGGREGATE_STATE, LogicalTypeId::ANY}, LogicalTypeId::AGGREGATE_STATE,
	                   AggregateStateCombine, BindAggregateState, nullptr, nullptr, InitCombineState);
	result.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	result.SetSerializeCallback(ExportStateScalarSerialize);
	result.SetDeserializeCallback(ExportStateScalarDeserialize);
	return result;
}

} // namespace duckdb
