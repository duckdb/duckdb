#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/list_vector.hpp"
#include "duckdb/common/vector/struct_vector.hpp"
#include "duckdb/common/types/list_segment.hpp"
#include "duckdb/function/aggregate_state_layout.hpp"
#include "duckdb/function/create_sort_key.hpp"
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

// Recursively serialize a state field to a result vector.
// base: accumulated byte offset from the state slot start to this field's parent base.
// Each child's field_offset is relative to that parent base.
static void SerializeField(const LogicalType &type, const AggregateStateField &field, Vector &result, idx_t count,
                           const data_ptr_t *addresses, idx_t base) {
	switch (field.kind) {
	case AggregateFieldKind::OPTIONAL_VALUE:
		D_ASSERT(field.children.size() == 1);
		for (idx_t i = 0; i < count; i++) {
			if (!Load<bool>(addresses[i] + base + field.field_offset)) {
				FlatVector::SetNull(result, i, true);
			}
		}
		SerializeField(type, field.children[0], result, count, addresses, base);
		break;
	case AggregateFieldKind::SORT_KEY:
		for (idx_t i = 0; i < count; i++) {
			if (!FlatVector::Validity(result).RowIsValid(i)) {
				continue;
			}
			const string_t sort_key = Load<string_t>(addresses[i] + base + field.field_offset);
			CreateSortKeyHelpers::DecodeSortKey(sort_key, result, i,
			                                    OrderModifiers(field.sort_key_order, OrderByNullType::NULLS_LAST));
		}
		break;
	case AggregateFieldKind::STRUCT: {
		const auto &child_types = StructType::GetChildTypes(type);
		auto &struct_entries = StructVector::GetEntries(result);
		const idx_t new_base = base + field.field_offset;
		for (idx_t field_idx = 0; field_idx < field.children.size(); field_idx++) {
			SerializeField(child_types[field_idx].second, field.children[field_idx], struct_entries[field_idx], count,
			               addresses, new_base);
		}
		break;
	}
	case AggregateFieldKind::PRIMITIVE:
		TemplateDispatch<StoreOp>(type.InternalType(), result, count, addresses, base + field.field_offset);
		break;
	case AggregateFieldKind::LIST: {
		// linked list field: build the result LIST vector from each state's linked list
		// an empty linked list is exported as NULL, matching the finalize semantics of list aggregates
		D_ASSERT(type.id() == LogicalTypeId::LIST);
		vector<LinkedList> linked_lists;
		linked_lists.reserve(count);
		for (idx_t i = 0; i < count; i++) {
			linked_lists.push_back(Load<LinkedList>(addresses[i] + base + field.field_offset));
		}
		field.list_functions.BuildLists(linked_lists, result, 0);
		break;
	}
	}
}

// Recursively deserialize an input vector into a packed state buffer.
// base: accumulated byte offset within each state slot for this field's parent base.
static void DeserializeField(const LogicalType &type, const AggregateStateField &field, const Vector &input_vec,
                             idx_t count, data_ptr_t dest_buffer, idx_t stride, idx_t base, ArenaAllocator &allocator) {
	switch (field.kind) {
	case AggregateFieldKind::OPTIONAL_VALUE: {
		D_ASSERT(field.children.size() == 1);
		const auto validity = input_vec.Validity();
		for (idx_t i = 0; i < count; i++) {
			Store<bool>(validity.IsValid(i), dest_buffer + i * stride + base + field.field_offset);
		}
		DeserializeField(type, field.children[0], input_vec, count, dest_buffer, stride, base, allocator);
		break;
	}
	case AggregateFieldKind::SORT_KEY: {
		Vector sort_keys(LogicalType::BLOB);
		CreateSortKeyHelpers::CreateSortKey(
		    input_vec, count, OrderModifiers(field.sort_key_order, OrderByNullType::NULLS_LAST), sort_keys);
		auto *key_data = FlatVector::GetData<string_t>(sort_keys);
		const auto validity = input_vec.Validity();
		for (idx_t i = 0; i < count; i++) {
			if (!validity.IsValid(i)) {
				continue;
			}
			auto sort_key = key_data[i];
			if (!sort_key.IsInlined()) {
				const auto len = sort_key.GetSize();
				auto *buf = char_ptr_cast(allocator.Allocate(len));
				memcpy(buf, sort_key.GetData(), len);
				sort_key = string_t(buf, UnsafeNumericCast<uint32_t>(len));
			}
			Store<string_t>(sort_key, dest_buffer + i * stride + base + field.field_offset);
		}
		break;
	}
	case AggregateFieldKind::STRUCT: {
		const auto &child_types = StructType::GetChildTypes(type);
		const auto &struct_entries = StructVector::GetEntries(input_vec);
		const idx_t new_base = base + field.field_offset;
		for (idx_t field_idx = 0; field_idx < field.children.size(); field_idx++) {
			DeserializeField(child_types[field_idx].second, field.children[field_idx], struct_entries[field_idx], count,
			                 dest_buffer, stride, new_base, allocator);
		}
		break;
	}
	case AggregateFieldKind::PRIMITIVE:
		TemplateDispatch<LoadOp>(type.InternalType(), stride, input_vec, count, dest_buffer, base + field.field_offset);
		break;
	case AggregateFieldKind::LIST: {
		// linked list field: append each row of the input LIST vector into the state's linked list
		D_ASSERT(type.id() == LogicalTypeId::LIST);
		// the child data is appended through the ListSegmentFunctions API, which takes a RecursiveUnifiedVectorFormat
		RecursiveUnifiedVectorFormat child_data;
		Vector::RecursiveToUnifiedFormat(ListVector::GetChild(input_vec), child_data);

		auto values = input_vec.Values<list_entry_t>();
		for (idx_t i = 0; i < count; i++) {
			LinkedList linked_list;
			const auto entry = values[i];
			if (entry.IsValid()) {
				// NULL inputs keep an empty linked list
				field.list_functions.AppendListEntry(allocator, linked_list, child_data, entry.GetValue());
			}
			Store<LinkedList>(linked_list, dest_buffer + i * stride + base + field.field_offset);
		}
		break;
	}
	}
}

static void DeserializeState(const AggregateStateLayout &layout, const Vector &input_vec, idx_t count,
                             data_ptr_t dest_buffer, ArenaAllocator &allocator) {
	DeserializeField(layout.type, layout.field, input_vec, count, dest_buffer, layout.total_state_size, 0, allocator);
}

static void SerializeState(const AggregateStateLayout &layout, Vector &result, idx_t count,
                           const data_ptr_t *addresses) {
	SerializeField(layout.type, layout.field, result, count, addresses, 0);
}

struct CombineState : public FunctionLocalState {
	//! The state layout, including the segment functions when the state is a linked list
	AggregateStateLayout layout;

	unsafe_unique_array<data_t> state_buffer0, state_buffer1;
	Vector addresses0, addresses1;

	ArenaAllocator allocator;

	explicit CombineState(const ExportAggregateBindData &bind_data)
	    : layout(GetLayout(bind_data.aggr)),
	      state_buffer0(make_unsafe_uniq_array<data_t>(STANDARD_VECTOR_SIZE * layout.total_state_size)),
	      state_buffer1(make_unsafe_uniq_array<data_t>(STANDARD_VECTOR_SIZE * layout.total_state_size)),
	      addresses0(LogicalType::POINTER), addresses1(LogicalType::POINTER), allocator(Allocator::DefaultAllocator()) {
	}
};

unique_ptr<FunctionLocalState> InitCombineState(ExpressionState &state, const BoundFunctionExpression &expr,
                                                FunctionData *bind_data_p) {
	auto &bind_data = bind_data_p->Cast<ExportAggregateBindData>();
	return make_uniq<CombineState>(bind_data);
}

struct FinalizeState : public FunctionLocalState {
	//! The state layout, including the segment functions when the state is a linked list
	AggregateStateLayout layout;

	unsafe_unique_array<data_t> state_buffer;
	Vector addresses;

	ArenaAllocator allocator;

	explicit FinalizeState(const ExportAggregateBindData &bind_data)
	    : layout(GetLayout(bind_data.aggr)),
	      state_buffer(make_unsafe_uniq_array<data_t>(STANDARD_VECTOR_SIZE * layout.total_state_size)),
	      addresses(LogicalType::POINTER), allocator(Allocator::DefaultAllocator()) {
	}
};

unique_ptr<FunctionLocalState> InitFinalizeState(ExpressionState &state, const BoundFunctionExpression &expr,
                                                 FunctionData *bind_data_p) {
	auto &bind_data = bind_data_p->Cast<ExportAggregateBindData>();
	return make_uniq<FinalizeState>(bind_data);
}

void AggregateStateFinalize(DataChunk &input, ExpressionState &state_p, Vector &result) {
	auto &bind_data = ExportAggregateBindData::GetFrom(state_p);
	auto &local_state = ExecuteFunctionState::GetFunctionState(state_p)->Cast<FinalizeState>();
	local_state.allocator.Reset();

	D_ASSERT(bind_data.state_size == bind_data.aggr.GetStateSizeCallback()(bind_data.aggr));
	D_ASSERT(input.data.size() == 1);

	auto &layout = local_state.layout;

	auto count = input.size();
	auto state_vec_writer = FlatVector::Writer<data_ptr_t>(local_state.addresses, count);
	for (idx_t i = 0; i < count; i++) {
		state_vec_writer.WriteValue(local_state.state_buffer.get() + i * layout.total_state_size);
	}

	DeserializeState(layout, input.data[0], count, local_state.state_buffer.get(), local_state.allocator);

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

	auto &layout = local_state.layout;
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
	DeserializeState(layout, input.data[0], count, local_state.state_buffer0.get(), local_state.allocator);
	DeserializeState(layout, input.data[1], count, local_state.state_buffer1.get(), local_state.allocator);

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

// the executor invokes this callback with combine_aggr's own bind data (ExportAggregateBindData) - the underlying
// aggregate's combine expects its own bind data, so we forward it here
void CombineAggrStateCombine(Vector &source, Vector &target, AggregateInputData &aggr_input_data, idx_t count) {
	auto &bind_data = aggr_input_data.bind_data->Cast<ExportAggregateBindData>();
	AggregateInputData combine_input(bind_data.aggr, bind_data.bind_data.get(), aggr_input_data.allocator,
	                                 aggr_input_data.combine_type);
	bind_data.aggr.GetStateCombineCallback()(source, target, combine_input, count);
}

unique_ptr<FunctionData> CombineAggrBind(BindAggregateFunctionInput &input) {
	auto &context = input.GetClientContext();
	auto &function = input.GetBoundFunction();
	auto &arguments = input.GetArguments();

	auto bind_data = BindAggregateStateInternal(context, function, arguments);

	// Copy underlying aggregate's callbacks into this function (same pattern as `ExportAggregateFunction::Bind`)
	function.SetStateSizeCallback(bind_data->aggr.GetStateSizeCallback());
	function.SetStateInitCallback(bind_data->aggr.GetStateInitCallback());
	function.SetStateCombineCallback(CombineAggrStateCombine);

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

	DeserializeState(layout, inputs[0], count, temp_state_buf.get(), aggr_input_data.allocator);

	AggregateInputData combine_input(bind_data.aggr, bind_data.bind_data.get(), aggr_input_data.allocator,
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
	// deep copy the type before modifying it - SetAlias/SetExtensionInfo modify the (shared) extra type info in
	// place, and the state layout type can share its type info with e.g. the aggregate's input expressions
	LogicalType state_layout = bound_function.GetStateType().type.DeepCopy();
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
