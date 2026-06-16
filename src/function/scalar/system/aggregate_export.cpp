#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/list_vector.hpp"
#include "duckdb/common/vector/struct_vector.hpp"
#include "duckdb/common/types/list_segment.hpp"
#include "duckdb/common/types/variant_value.hpp"
#include "duckdb/function/aggregate_state_layout.hpp"
#include "duckdb/function/create_sort_key.hpp"
#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "duckdb/common/extension_type_info.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
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

static AggregateStateLayout GetLayout(const BoundAggregateFunction &aggr, optional_ptr<FunctionData> bind_data) {
	return aggr.GetStateType(bind_data);
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
		D_ASSERT(field.children.size() == 1);
		vector<LinkedList> linked_lists;
		linked_lists.reserve(count);
		for (idx_t i = 0; i < count; i++) {
			linked_lists.push_back(Load<LinkedList>(addresses[i] + base + field.field_offset));
		}
		const auto &element = field.children[0];
		if (element.kind != AggregateFieldKind::SORT_KEY) {
			// elements are stored directly - build the result LIST vector from each state's linked list
			field.list_functions.BuildLists(linked_lists, result, 0);
			break;
		}
		// the elements are sort keys: build the physically stored (BLOB) elements into a temporary LIST vector, then
		// decode each sort key into the result child while rebuilding the result's list entries
		Vector physical_list(LogicalType::LIST(LogicalType::BLOB), count);
		field.list_functions.BuildLists(linked_lists, physical_list, 0);

		ListVector::Reserve(result, ListVector::GetListSize(physical_list));
		auto &result_child = ListVector::GetChildMutable(result);
		auto result_entries = FlatVector::GetDataMutable<list_entry_t>(result);
		const OrderModifiers modifiers(element.sort_key_order, OrderByNullType::NULLS_LAST);

		idx_t child_offset = 0;
		for (const auto list_entry : physical_list.Values<VectorListType<string_t>>()) {
			const auto row = list_entry.GetIndex();
			if (!list_entry.IsValid()) {
				// an empty linked list is exported as NULL, matching the finalize semantics of list aggregates
				FlatVector::SetNull(result, row, true);
				result_entries[row] = {child_offset, 0};
				continue;
			}
			result_entries[row] = {child_offset, list_entry.GetListLength()};
			for (const auto sort_key : list_entry.GetChildValues()) {
				CreateSortKeyHelpers::DecodeSortKey(sort_key.GetValueUnsafe(), result_child, child_offset++, modifiers);
			}
		}
		ListVector::SetListSize(result, child_offset);
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
		D_ASSERT(field.children.size() == 1);
		const auto values = input_vec.Values<list_entry_t>();
		const auto &element = field.children[0];
		const auto &logical_child = ListVector::GetChild(input_vec);

		// the child is appended through the ListSegmentFunctions API, which physically stores the element type -
		// sort-key elements are first re-encoded from the logical child into a temporary BLOB child vector
		optional_ptr<const Vector> physical_child = logical_child;
		unique_ptr<Vector> encoded_child;
		if (element.kind == AggregateFieldKind::SORT_KEY) {
			const auto child_count = ListVector::GetListSize(input_vec);
			const OrderModifiers modifiers(element.sort_key_order, OrderByNullType::NULLS_LAST);
			// the result must be sized for the full (possibly larger than standard) child up front
			encoded_child = make_uniq<Vector>(LogicalType::BLOB, MaxValue<idx_t>(child_count, 1));
			CreateSortKeyHelpers::CreateSortKey(logical_child, child_count, modifiers, *encoded_child);
			physical_child = *encoded_child;
		}

		RecursiveUnifiedVectorFormat child_data;
		Vector::RecursiveToUnifiedFormat(*physical_child, child_data);
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

static void DeserializeState(const BoundAggregateFunction &aggr, const AggregateStateLayout &layout,
                             const Vector &input_vec, idx_t count, data_ptr_t dest_buffer, ArenaAllocator &allocator) {
	if (aggr.HasImportAggregateStateCallback()) {
		// the aggregate explicitly deserializes its own states
		AggregateImportInputData import_input(layout, input_vec, dest_buffer, allocator);
		aggr.GetImportAggregateStateCallback()(import_input);
		return;
	}
	DeserializeField(layout.type, layout.field, input_vec, count, dest_buffer, layout.total_state_size, 0, allocator);
}

static void SerializeState(const AggregateStateLayout &layout, Vector &result, idx_t count,
                           const data_ptr_t *addresses) {
	SerializeField(layout.type, layout.field, result, count, addresses, 0);
}

static void SerializeState(const BoundAggregateFunction &aggr, optional_ptr<FunctionData> bind_data,
                           const AggregateStateLayout &layout, Vector &states, idx_t count, Vector &result,
                           ArenaAllocator &allocator) {
	if (aggr.HasExportAggregateStateCallback()) {
		// the aggregate explicitly serializes its own states
		AggregateFinalizeInputData aggr_input_data(aggr, bind_data, allocator);
		aggr.GetExportAggregateStateCallback()(states, aggr_input_data, result, count, 0);
		return;
	}
	const data_ptr_t *addresses;
	if (states.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		addresses = ConstantVector::GetData<data_ptr_t>(states);
	} else {
		addresses = FlatVector::GetData<data_ptr_t>(states);
	}
	SerializeState(layout, result, count, addresses);
}

// destroys the temporary underlying-aggregate states referenced by `states` (no-op if the aggregate has no
// destructor). the combine/finalize paths below initialize underlying states into scratch buffers - without this
// their owned heap (e.g. approx_top_k's hash map, approx_quantile's t-digest) would leak.
static void DestroyExportStates(const BoundAggregateFunction &aggr, optional_ptr<FunctionData> bind_data,
                                Vector &states, idx_t count, ArenaAllocator &allocator) {
	if (count == 0 || !aggr.HasStateDestructorCallback()) {
		return;
	}
	AggregateInputData aggr_input_data(aggr, bind_data, allocator);
	aggr.GetStateDestructorCallback()(states, aggr_input_data, count);
}

struct CombineState : public FunctionLocalState {
	//! The state layout, including the segment functions when the state is a linked list
	AggregateStateLayout layout;

	unsafe_unique_array<data_t> state_buffer0, state_buffer1;
	Vector addresses0, addresses1;

	ArenaAllocator allocator;

	explicit CombineState(const ExportAggregateBindData &bind_data)
	    : layout(GetLayout(bind_data.aggr, bind_data.bind_data.get())),
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
	    : layout(GetLayout(bind_data.aggr, bind_data.bind_data.get())),
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

	DeserializeState(bind_data.aggr, layout, input.data[0], count, local_state.state_buffer.get(),
	                 local_state.allocator);

	AggregateFinalizeInputData aggr_input_data(bind_data.aggr, bind_data.bind_data.get(), local_state.allocator);
	bind_data.aggr.GetStateFinalizeCallback()(local_state.addresses, aggr_input_data, result, count, 0);
	DestroyExportStates(bind_data.aggr, bind_data.bind_data.get(), local_state.addresses, count, local_state.allocator);

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
	DeserializeState(bind_data.aggr, layout, input.data[0], count, local_state.state_buffer0.get(),
	                 local_state.allocator);
	DeserializeState(bind_data.aggr, layout, input.data[1], count, local_state.state_buffer1.get(),
	                 local_state.allocator);

	AggregateInputData aggr_input_data(bind_data.aggr, bind_data.bind_data.get(), local_state.allocator,
	                                   AggregateCombineType::ALLOW_DESTRUCTIVE);
	// the combine/serialize may throw (e.g. combining approx_top_k states with different k) - always destroy the
	// scratch states so their owned heap is not leaked
	auto destroy_states = [&]() {
		DestroyExportStates(bind_data.aggr, bind_data.bind_data.get(), local_state.addresses0, count,
		                    local_state.allocator);
		DestroyExportStates(bind_data.aggr, bind_data.bind_data.get(), local_state.addresses1, count,
		                    local_state.allocator);
	};
	try {
		bind_data.aggr.GetStateCombineCallback()(local_state.addresses0, local_state.addresses1, aggr_input_data,
		                                         count);
		SerializeState(bind_data.aggr, bind_data.bind_data.get(), layout, local_state.addresses1, count, result,
		               local_state.allocator);
	} catch (...) {
		destroy_states();
		throw;
	}
	destroy_states();

	// Rows where both inputs were NULL produce no meaningful combined state — mark result as NULL
	for (idx_t i = 0; i < count; i++) {
		if (!validity0.IsValid(i) && !validity1.IsValid(i)) {
			FlatVector::SetNull(result, i, true);
		}
	}
}

// looks up the aggregate function with the given name in the catalog and binds it with the given argument types
// constant_parameters holds the values of arguments that must be re-bound with a specific constant
// (e.g. string_agg's separator), keyed by argument index - all other arguments are bound with a NULL value
unique_ptr<ExportAggregateBindData> BindExportedAggregate(ClientContext &context, const string &function_name,
                                                          const vector<LogicalType> &argument_types,
                                                          const map<idx_t, Value> &constant_parameters) {
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
	for (idx_t arg_idx = 0; arg_idx < argument_types.size(); arg_idx++) {
		auto constant_entry = constant_parameters.find(arg_idx);
		if (constant_entry != constant_parameters.end()) {
			args.push_back(make_uniq<BoundConstantExpression>(constant_entry->second));
		} else {
			args.push_back(make_uniq<BoundConstantExpression>(Value(argument_types[arg_idx])));
		}
	}

	auto [bound_aggr, bind_info] = function_binder.ResolveFunction(aggr, args);

	// the bind callback can erase constant arguments (e.g. string_agg's separator) - in that case the original
	// argument list holds the pre-erase arguments that the exported signature refers to
	const auto &bound_args =
	    bound_aggr.GetOriginalArguments().empty() ? bound_aggr.GetArguments() : bound_aggr.GetOriginalArguments();
	bool signature_matches = bound_args.size() == argument_types.size();
	for (idx_t arg_idx = 0; signature_matches && arg_idx < bound_args.size(); arg_idx++) {
		// an ANY argument in the function signature (e.g. string_agg's data argument) matches any requested type
		if (bound_args[arg_idx].id() != LogicalTypeId::ANY && bound_args[arg_idx] != argument_types[arg_idx]) {
			signature_matches = false;
		}
	}
	if (!signature_matches) {
		throw InternalException("Type mismatch for exported aggregate %s: bound=[%s] requested=[%s]", function_name,
		                        StringUtil::ToString(bound_args, ", "), StringUtil::ToString(argument_types, ", "));
	}

	return make_uniq<ExportAggregateBindData>(bound_aggr, std::move(bind_info),
	                                          bound_aggr.GetStateSizeCallback()(bound_aggr));
}

// parses the "parameters" property of an AGGREGATE_STATE type
// each parameter is either a plain type, or a (type, value) pair for parameters that were bound to a constant
// (e.g. string_agg's separator) - the latter are returned in constant_parameters, keyed by argument index
void ParseStateParameters(const Value &parameters, vector<LogicalType> &argument_types,
                          map<idx_t, Value> &constant_parameters) {
	for (auto &val : ListValue::GetChildren(parameters)) {
		const idx_t arg_idx = argument_types.size();
		if (!val.IsNull() && val.type().id() == LogicalTypeId::TYPE) {
			// plain type
			argument_types.push_back(TypeValue::GetType(val));
			continue;
		}
		if (!val.IsNull() && val.type().id() == LogicalTypeId::STRUCT) {
			// (type, value) pair
			auto &children = StructValue::GetChildren(val);
			if (children.size() != 2 || children[0].IsNull() || children[0].type().id() != LogicalTypeId::TYPE ||
			    children[1].type().id() != LogicalTypeId::VARIANT) {
				throw InternalException("Aggregate state parameter entry should be a (type, value) pair");
			}
			argument_types.push_back(TypeValue::GetType(children[0]));
			if (!children[1].IsNull()) {
				// the parameter is bound to a constant - decode it as-is, without casting it to the declared
				// argument type, so that re-binding sees the same (pre-cast) constant as the original bind
				// (e.g. a DECIMAL quantile parameter must stay DECIMAL even though the signature says DOUBLE)
				constant_parameters.emplace(arg_idx, VariantValue::GetValue(children[1]));
			}
			continue;
		}
		throw InternalException(
		    "Aggregate state object should have a property called parameters that is a list of types");
	}
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
	map<idx_t, Value> constant_parameters;
	ParseStateParameters(entry->second, argument_types, constant_parameters);

	return BindExportedAggregate(context, function_name, argument_types, constant_parameters);
}

unique_ptr<FunctionData> BindAggregateState(BindScalarFunctionInput &input) {
	auto &bound_function = input.GetBoundFunction();
	auto &arguments = input.GetArguments();
	auto bind_data =
	    BindAggregateStateInternal(input.GetClientContext(), input.GetBoundFunction(), input.GetArguments());

	// combine - both arguments must be aggregate states of the same function with the same signature
	if (arguments.size() == 2 && arguments[0]->GetReturnType() != arguments[1]->GetReturnType()) {
		auto &left_type = arguments[0]->GetReturnType();
		auto &right_type = arguments[1]->GetReturnType();
		if (left_type.IsAggregateState() && right_type.IsAggregateState()) {
			// both are aggregate states but they are not equal - if the function and signature match, the states
			// were bound with different constant parameters (e.g. string_agg states with different separators)
			auto &left_props = left_type.GetExtensionInfo()->properties;
			auto &right_props = right_type.GetExtensionInfo()->properties;
			auto left_name = left_props.find("function_name");
			auto right_name = right_props.find("function_name");
			if (left_name != left_props.end() && right_name != right_props.end() &&
			    left_name->second == right_name->second) {
				auto left_params = left_props.find("parameters");
				auto right_params = right_props.find("parameters");
				throw BinderException(
				    "Cannot COMBINE aggregate states of \"%s\" that were created with different parameters: %s <> %s",
				    StringValue::Get(left_name->second),
				    left_params == left_props.end() ? "?" : left_params->second.ToString(),
				    right_params == right_props.end() ? "?" : right_params->second.ToString());
			}
		}
		throw BinderException("Cannot COMBINE aggregate states from different functions, %s <> %s",
		                      left_type.ToString(), right_type.ToString());
	}

	if (bound_function.GetName() == "finalize") {
		bound_function.SetReturnType(bind_data->aggr.GetReturnType());
	} else {
		D_ASSERT(bound_function.GetName() == "combine");
		bound_function.SetReturnType(arguments[0]->GetReturnType());
	}

	return std::move(bind_data);
}

void ExportAggregateFinalize(Vector &state, AggregateFinalizeInputData &aggr_input_data, Vector &result, idx_t count,
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

	auto layout = GetLayout(aggr_input_data.function, aggr_input_data.bind_data);

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

// destroys combine_aggr's accumulator states by forwarding to the underlying aggregate's destructor (with its own bind
// data) - otherwise states that own heap (e.g. approx_top_k's hash map, approx_quantile's t-digest) would leak
void CombineAggrStateDestroy(Vector &state, AggregateInputData &aggr_input_data, idx_t count) {
	auto &bind_data = aggr_input_data.bind_data->Cast<ExportAggregateBindData>();
	AggregateInputData destroy_input(bind_data.aggr, bind_data.bind_data.get(), aggr_input_data.allocator);
	bind_data.aggr.GetStateDestructorCallback()(state, destroy_input, count);
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
	if (bind_data->aggr.HasStateDestructorCallback()) {
		function.SetStateDestructorCallback(CombineAggrStateDestroy);
	}

	function.SetReturnType(arguments[0]->GetReturnType());

	return std::move(bind_data);
}

void CombineAggrUpdate(Vector inputs[], AggregateInputData &aggr_input_data, idx_t input_count, Vector &states,
                       idx_t count) {
	D_ASSERT(input_count == 1);

	auto &bind_data = aggr_input_data.bind_data->Cast<ExportAggregateBindData>();
	auto &underlying_aggr = bind_data.aggr;

	auto layout = GetLayout(underlying_aggr, bind_data.bind_data.get());

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

	DeserializeState(underlying_aggr, layout, inputs[0], count, temp_state_buf.get(), aggr_input_data.allocator);

	AggregateInputData combine_input(bind_data.aggr, bind_data.bind_data.get(), aggr_input_data.allocator,
	                                 AggregateCombineType::ALLOW_DESTRUCTIVE);
	underlying_aggr.GetStateCombineCallback()(source_vec, target_vec, combine_input, count);
	// the target states are the real combine_aggr states (kept); the source states are scratch - destroy them
	DestroyExportStates(underlying_aggr, bind_data.bind_data.get(), source_vec, count, aggr_input_data.allocator);
}

void CombineAggrFinalize(Vector &state, AggregateFinalizeInputData &aggr_input_data, Vector &result, idx_t count,
                         idx_t offset) {
	D_ASSERT(offset == 0);
	auto &bind_data = aggr_input_data.bind_data->Cast<ExportAggregateBindData>();
	auto &underlying_aggr = bind_data.aggr;

	auto layout = GetLayout(underlying_aggr, bind_data.bind_data.get());

	result.Flatten();
	SerializeState(underlying_aggr, bind_data.bind_data.get(), layout, state, count, result, aggr_input_data.allocator);
}

// constructs the AGGREGATE_STATE type for the given bound aggregate function
// the state layout (a struct) is aliased to AGGREGATE_STATE, with the function name and signature stored in the
// extension type info so that the aggregate can be re-bound later (e.g. by FINALIZE/COMBINE)
LogicalType CreateAggregateStateType(const BoundAggregateFunction &bound_function,
                                     optional_ptr<FunctionData> bind_data) {
	auto layout = bound_function.GetStateType(bind_data);
	// deep copy the type before modifying it - SetAlias/SetExtensionInfo modify the (shared) extra type info in
	// place, and the state layout type can share its type info with e.g. the aggregate's input expressions
	LogicalType state_layout = layout.type.DeepCopy();
	state_layout.SetAlias("AGGREGATE_STATE");
	auto ext_info = make_uniq<ExtensionTypeInfo>();
	ext_info->properties.emplace("function_name", bound_function.GetName());
	auto &original_arguments = bound_function.GetOriginalArguments().empty() ? bound_function.GetArguments()
	                                                                         : bound_function.GetOriginalArguments();
	vector<Value> arguments;
	if (layout.constant_parameters.empty()) {
		// all parameters are plain types - store the parameters as a list of types
		for (auto &arg : original_arguments) {
			arguments.push_back(Value::TYPE(arg));
		}
		ext_info->properties.emplace("parameters", Value::LIST(LogicalType::TYPE(), std::move(arguments)));
	} else {
		// some parameters were bound to a constant (e.g. string_agg's separator) - store the parameters as a list of
		// (type, value) pairs, where the value holds the constant the parameter must be re-bound with
		for (idx_t arg_idx = 0; arg_idx < original_arguments.size(); arg_idx++) {
			child_list_t<Value> children;
			children.emplace_back("type", Value::TYPE(original_arguments[arg_idx]));
			auto constant_entry = layout.constant_parameters.find(arg_idx);
			if (constant_entry == layout.constant_parameters.end()) {
				children.emplace_back("value", Value(LogicalType::VARIANT()));
			} else {
				children.emplace_back("value", constant_entry->second.DefaultCastAs(LogicalType::VARIANT()));
			}
			arguments.push_back(Value::STRUCT(std::move(children)));
		}
		auto entry_type = LogicalType::STRUCT({{"type", LogicalType::TYPE()}, {"value", LogicalType::VARIANT()}});
		ext_info->properties.emplace("parameters", Value::LIST(entry_type, std::move(arguments)));
	}
	state_layout.SetExtensionInfo(std::move(ext_info));
	return state_layout;
}

// parses a single entry of the to_aggregate_state signature list - either a TYPE value (e.g. make_type('VARCHAR'))
// or a string naming a type (e.g. 'VARCHAR')
LogicalType ParseSignatureType(ClientContext &context, const Value &arg) {
	if (arg.IsNull()) {
		throw BinderException("to_aggregate_state: the signature cannot contain NULL values");
	}
	if (arg.type().id() == LogicalTypeId::TYPE) {
		return TypeValue::GetType(arg);
	}
	if (arg.type().id() == LogicalTypeId::VARCHAR) {
		return TransformStringToLogicalType(StringValue::Get(arg), context);
	}
	throw BinderException("to_aggregate_state: the signature must be a list of types");
}

// parses the optional fourth argument of to_aggregate_state: the constant values for arguments that must be bound to
// a specific constant rather than a NULL value of the argument type (e.g. string_agg's separator). It is supplied as
// a LIST with one entry per argument (i.e. the same length as the signature): NULL for arguments that are not bound to
// a constant, and the constant value for arguments that are, e.g. [NULL, '|'] to bind argument 1 to the constant '|'
void ParseConstantParameters(const Value &constants, idx_t argument_count, map<idx_t, Value> &constant_parameters) {
	if (constants.IsNull()) {
		return;
	}
	if (constants.type().id() != LogicalTypeId::LIST) {
		throw BinderException("to_aggregate_state: the constant parameters must be a list with one entry per argument "
		                      "(use NULL for arguments that are not bound to a constant), e.g. [NULL, '|']");
	}
	auto &children = ListValue::GetChildren(constants);
	if (children.size() != argument_count) {
		throw BinderException("to_aggregate_state: the constant parameters list has %llu entries but the aggregate has "
		                      "%llu arguments - it must have exactly one entry per argument (use NULL for arguments "
		                      "that are not bound to a constant)",
		                      children.size(), argument_count);
	}
	for (idx_t i = 0; i < children.size(); i++) {
		if (children[i].IsNull()) {
			continue;
		}
		constant_parameters[i] = children[i];
	}
}

unique_ptr<FunctionData> ToAggregateStateBind(BindScalarFunctionInput &input) {
	auto &bound_function = input.GetBoundFunction();
	auto &arguments = input.GetArguments();
	auto &context = input.GetClientContext();
	for (idx_t i = 1; i < arguments.size(); i++) {
		if (arguments[i]->HasParameter()) {
			throw ParameterNotResolvedException();
		}
		if (!arguments[i]->IsFoldable()) {
			throw BinderException("to_aggregate_state: the aggregate name, signature and constant parameters must be "
			                      "constant");
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
	// the signature lists all of the argument types in order
	vector<LogicalType> argument_types;
	for (auto &arg : ListValue::GetChildren(signature_val)) {
		argument_types.push_back(ParseSignatureType(context, arg));
	}

	// constant_parameters holds the values of arguments that must be re-bound with a specific constant rather than a
	// NULL value of the argument type (e.g. string_agg's separator), keyed by the argument's index
	map<idx_t, Value> constant_parameters;
	if (arguments.size() > 3) {
		auto constants_val = ExpressionExecutor::EvaluateScalar(context, *arguments[3]);
		ParseConstantParameters(constants_val, argument_types.size(), constant_parameters);
	}
	for (auto &entry : constant_parameters) {
		// cast each constant to the argument type declared in the signature
		entry.second = entry.second.DefaultCastAs(argument_types[entry.first]);
	}

	auto bind_data = BindExportedAggregate(context, function_name, argument_types, constant_parameters);
	auto &aggr = bind_data->aggr;
	if (!aggr.HasGetStateTypeCallback()) {
		throw BinderException(
		    "Aggregate function \"%s\" does not have a state type callback defined - cannot convert to its state",
		    function_name);
	}
	auto state_layout = aggr.GetStateType(bind_data->bind_data.get()).type;
	bound_function.GetArguments()[0] = state_layout;
	bound_function.SetReturnType(CreateAggregateStateType(aggr, bind_data->bind_data.get()));
	return std::move(bind_data);
}

void ToAggregateStateFunction(DataChunk &input, ExpressionState &state, Vector &result) {
	// the input type is verified to match the state layout at bind time - we only need to reinterpret the vector
	result.Reinterpret(input.data[0]);
}

} // namespace

void ExportAggregateFunction::SetStateExport(BoundAggregateExpression &aggregate, LogicalType state_layout) {
	auto &bound_function = aggregate.FunctionMutable();
	// functions with an explicit export callback use it as the finalize; others use the field-based serialization
	bound_function.SetStateFinalizeCallback(bound_function.HasExportAggregateStateCallback()
	                                            ? bound_function.GetExportAggregateStateCallback()
	                                            : ExportAggregateFinalize);
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
	if (bound_function.HasExportAggregateStateCallback() != bound_function.HasImportAggregateStateCallback()) {
		throw InternalException("Aggregate function \"%s\" must define either both or neither of the "
		                        "export_aggregate_state/import_aggregate_state callbacks",
		                        bound_function.GetName());
	}
	if (!bound_function.HasGetStateTypeCallback()) {
		throw NotImplementedException(
		    "Aggregate function \"%s\" does not have a state type callback defined - cannot export state",
		    bound_function.GetName());
	}
	D_ASSERT(bound_function.HasStateSizeCallback());
	D_ASSERT(bound_function.HasStateFinalizeCallback());
	D_ASSERT(child_aggregate->Function().GetReturnType().id() != LogicalTypeId::INVALID);
	SetStateExport(*child_aggregate, CreateAggregateStateType(bound_function, child_aggregate->BindInfo().get()));
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

ScalarFunctionSet ToAggregateStateFun::GetFunctions() {
	ScalarFunctionSet set("to_aggregate_state");
	vector<LogicalType> arguments {LogicalTypeId::ANY, LogicalType::VARCHAR, LogicalType::LIST(LogicalType::ANY)};
	for (idx_t constant_params = 0; constant_params < 2; constant_params++) {
		if (constant_params) {
			// optional fourth argument: constant parameter values as a list with one entry per argument (e.g.
			// [NULL, ','])
			arguments.emplace_back(LogicalTypeId::ANY);
		}
		ScalarFunction function("to_aggregate_state", arguments, LogicalTypeId::ANY, ToAggregateStateFunction,
		                        ToAggregateStateBind);
		function.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
		set.AddFunction(std::move(function));
	}
	return set;
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
