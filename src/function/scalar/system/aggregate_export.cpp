#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/struct_vector.hpp"
#include "duckdb/function/aggregate_state_layout.hpp"
#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "duckdb/common/extension_type_info.hpp"
#include "duckdb/common/types/value.hpp"
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

// Load rows from input_vec into the packed binary state buffer. Skips null rows.
struct LoadOp {
	template <class T>
	static void Operation(idx_t root_stride, const Vector &input_vec, idx_t count, data_ptr_t base_ptr,
	                      idx_t field_offset) {
		auto values = input_vec.Values<T>();
		for (idx_t i = 0; i < count; i++) {
			const auto entry = values[i];
			if (entry.IsValid()) {
				Store(entry.GetValue(), base_ptr + i * root_stride + field_offset);
			}
		}
	}
};

// Store rows from the packed binary state buffer into a result vector.
struct StoreOp {
	template <class T>
	static void Operation(Vector &result, idx_t count, const data_ptr_t *sources, idx_t field_offset) {
		auto dst = FlatVector::Writer<T>(result, count);
		for (idx_t i = 0; i < count; i++) {
			dst.WriteValue(Load<T>(sources[i] + field_offset));
		}
	}
};

// Serialize an OptionalStateType<T> state buffer (flat: T value at +0, bool is_set at +sizeof(T))
// into a result vector, setting NULL when is_set=false.
struct StorePrimitiveOptionalOp {
	template <class T>
	static void Operation(Vector &result, idx_t count, const data_ptr_t *addresses, idx_t base_offset) {
		auto dst = FlatVector::Writer<T>(result, count);
		for (idx_t i = 0; i < count; i++) {
			const auto value = Load<T>(addresses[i] + base_offset);
			const bool is_set = Load<bool>(addresses[i] + base_offset + sizeof(T));
			if (is_set) {
				dst.WriteValue(value);
			} else {
				dst.WriteNull();
			}
		}
	}
};

// Deserialize a nullable input vector into OptionalStateType<T> state buffer slots
// (flat: T value at +0, bool is_set at +sizeof(T)).
struct LoadPrimitiveOptionalOp {
	template <class T>
	static void Operation(const Vector &input, idx_t count, data_ptr_t base_ptr, idx_t aligned_state_size) {
		auto values = input.Values<T>();
		for (idx_t i = 0; i < count; i++) {
			const auto entry = values[i];
			auto *value_ptr = reinterpret_cast<T *>(base_ptr + i * aligned_state_size);
			auto *is_set_ptr = reinterpret_cast<bool *>(base_ptr + i * aligned_state_size + sizeof(T));
			if (entry.IsValid()) {
				*value_ptr = entry.GetValue();
				*is_set_ptr = true;
			} else {
				*is_set_ptr = false;
			}
		}
	}
};

void DeserializeState(const AggregateStateLayout &layout, const Vector &input_vec, idx_t count,
                      data_ptr_t dest_buffer) {
	if (layout.field.is_optional && layout.type.id() == LogicalTypeId::STRUCT) {
		// Optional struct: deserialize children, then derive is_set from the struct's row validity.
		const idx_t struct_size = AggregateStateField::GetPhysicalSize(layout.type);
		const auto &child_types = StructType::GetChildTypes(layout.type);
		const auto &struct_entries = StructVector::GetEntries(input_vec);
		for (idx_t field_idx = 0; field_idx < layout.field.children.size(); field_idx++) {
			const auto &child = layout.field.children[field_idx];
			AggregateStateLayout child_layout(child_types[field_idx].second, layout.total_state_size,
			                                  child.is_optional);
			DeserializeState(child_layout, struct_entries[field_idx], count, dest_buffer + child.field_offset);
		}
		auto &validity = FlatVector::Validity(input_vec);
		for (idx_t i = 0; i < count; i++) {
			*reinterpret_cast<bool *>(dest_buffer + i * layout.total_state_size + struct_size) = validity.RowIsValid(i);
		}
	} else if (layout.type.id() == LogicalTypeId::STRUCT) {
		const auto &child_types = StructType::GetChildTypes(layout.type);
		const auto &struct_entries = StructVector::GetEntries(input_vec);
		for (idx_t field_idx = 0; field_idx < layout.field.children.size(); field_idx++) {
			const auto &child = layout.field.children[field_idx];
			AggregateStateLayout child_layout(child_types[field_idx].second, layout.total_state_size,
			                                  child.is_optional);
			DeserializeState(child_layout, struct_entries[field_idx], count, dest_buffer + child.field_offset);
		}
	} else if (layout.field.is_optional) {
		TemplateDispatch<LoadPrimitiveOptionalOp>(layout.type.InternalType(), input_vec, count, dest_buffer,
		                                          layout.total_state_size);
	} else {
		TemplateDispatch<LoadOp>(layout.type.InternalType(), layout.total_state_size, input_vec, count, dest_buffer, 0);
	}
}

void SerializeState(const AggregateStateLayout &layout, Vector &result, idx_t count, const data_ptr_t *addresses,
                    idx_t base_offset = 0) {
	if (layout.field.is_optional && layout.type.id() == LogicalTypeId::STRUCT) {
		// Optional struct: mark null rows in the struct's validity, then serialize children for all rows.
		const idx_t struct_size = AggregateStateField::GetPhysicalSize(layout.type);
		const auto &child_types = StructType::GetChildTypes(layout.type);
		auto &struct_entries = StructVector::GetEntries(result);
		for (idx_t i = 0; i < count; i++) {
			if (!Load<bool>(addresses[i] + base_offset + struct_size)) {
				FlatVector::SetNull(result, i, true);
			}
		}
		for (idx_t field_idx = 0; field_idx < layout.field.children.size(); field_idx++) {
			const auto &child = layout.field.children[field_idx];
			AggregateStateLayout child_layout(child_types[field_idx].second, 0, child.is_optional);
			SerializeState(child_layout, struct_entries[field_idx], count, addresses, base_offset + child.field_offset);
		}
	} else if (layout.type.id() == LogicalTypeId::STRUCT) {
		const auto &child_types = StructType::GetChildTypes(layout.type);
		auto &struct_entries = StructVector::GetEntries(result);
		for (idx_t field_idx = 0; field_idx < layout.field.children.size(); field_idx++) {
			const auto &child = layout.field.children[field_idx];
			AggregateStateLayout child_layout(child_types[field_idx].second, 0, child.is_optional);
			SerializeState(child_layout, struct_entries[field_idx], count, addresses, base_offset + child.field_offset);
		}
	} else if (layout.field.is_optional) {
		TemplateDispatch<StorePrimitiveOptionalOp>(layout.type.InternalType(), result, count, addresses, base_offset);
	} else {
		TemplateDispatch<StoreOp>(layout.type.InternalType(), result, count, addresses, base_offset);
	}
}

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

	auto count = input.size();
	auto state_vec_writer = FlatVector::Writer<data_ptr_t>(local_state.addresses, count);
	for (idx_t i = 0; i < count; i++) {
		state_vec_writer.WriteValue(local_state.state_buffer.get() + i * layout.total_state_size);
	}

	DeserializeState(layout, input.data[0], count, local_state.state_buffer.get());

	AggregateInputData aggr_input_data(bind_data.aggr, bind_data.bind_data.get(), local_state.allocator);
	bind_data.aggr.GetStateFinalizeCallback()(local_state.addresses, aggr_input_data, result, count, 0);

	auto validity = input.data[0].Validity();
	for (idx_t i = 0; i < count; i++) {
		if (!validity.IsValid(i)) {
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
	D_ASSERT(input.data[0].GetType() == result.GetType());

	if (input.data[0].GetType().InternalType() != input.data[1].GetType().InternalType()) {
		throw IOException("Aggregate state combine type mismatch, expect %s, got %s",
		                  input.data[0].GetType().ToString(), input.data[1].GetType().ToString());
	}

	auto layout = GetLayout(bind_data.aggr);
	auto count = input.size();

	result.Flatten();

	auto validity0 = input.data[0].Validity();
	auto validity1 = input.data[1].Validity();

	// Initialize both state buffers and build address vectors for all rows
	auto state0_writer = FlatVector::Writer<data_ptr_t>(local_state.addresses0, count);
	auto state1_writer = FlatVector::Writer<data_ptr_t>(local_state.addresses1, count);
	for (idx_t i = 0; i < count; i++) {
		auto ptr0 = local_state.state_buffer0.get() + i * layout.total_state_size;
		auto ptr1 = local_state.state_buffer1.get() + i * layout.total_state_size;
		bind_data.aggr.GetStateInitCallback()(bind_data.aggr, ptr0);
		bind_data.aggr.GetStateInitCallback()(bind_data.aggr, ptr1);
		state0_writer.WriteValue(ptr0);
		state1_writer.WriteValue(ptr1);
	}

	// Deserialize both inputs — null rows are skipped by LoadOp, keeping the initialized empty state
	DeserializeState(layout, input.data[0], count, local_state.state_buffer0.get());
	DeserializeState(layout, input.data[1], count, local_state.state_buffer1.get());

	AggregateInputData aggr_input_data(bind_data.aggr, bind_data.bind_data.get(), local_state.allocator,
	                                   AggregateCombineType::ALLOW_DESTRUCTIVE);
	bind_data.aggr.GetStateCombineCallback()(local_state.addresses0, local_state.addresses1, aggr_input_data, count);

	SerializeState(layout, result, count, FlatVector::GetData<data_ptr_t>(local_state.addresses1));

	// Rows where both inputs were NULL produce no meaningful combined state — mark result as NULL
	for (idx_t i = 0; i < count; i++) {
		if (!validity0.IsValid(i) && !validity1.IsValid(i)) {
			FlatVector::SetNull(result, i, true);
		}
	}
}

// looks up the aggregate function with the given name in the catalog and binds it with the given argument types
unique_ptr<ExportAggregateBindData> BindExportedAggregate(ClientContext &context, const string &function_name,
                                                          const vector<LogicalType> &argument_types) {
	auto &func = Catalog::GetSystemCatalog(context).GetEntry<AggregateFunctionCatalogEntry>(
	    context, Identifier::DefaultSchema(), Identifier(function_name));
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
	SerializeState(layout, result, count, addresses_ptrs);
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

	auto aligned_size = layout.total_state_size;
	unsafe_unique_array<data_t> temp_state_buf = make_unsafe_uniq_array<data_t>(count * aligned_size);

	auto state_values = states.Values<data_ptr_t>();

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
		target_data.WriteValue(state_values[i].GetValue());
	}

	DeserializeState(layout, inputs[0], count, temp_state_buf.get());

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
	SerializeState(layout, result, count, addresses_ptrs);
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
