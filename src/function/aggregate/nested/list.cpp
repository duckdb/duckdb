#include "duckdb/common/pair.hpp"
#include "duckdb/common/types/chunk_collection.hpp"
#include "duckdb/common/types/list_segment.hpp"
#include "duckdb/function/aggregate/nested_functions.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"

namespace duckdb {

static void InitializeValidities(Vector &vector, idx_t &capacity) {

	auto &validity_mask = FlatVector::Validity(vector);
	validity_mask.Initialize(capacity);

	auto internal_type = vector.GetType().InternalType();
	if (internal_type == PhysicalType::LIST) {
		auto &child_vector = ListVector::GetEntry(vector);
		InitializeValidities(child_vector, capacity);
	} else if (internal_type == PhysicalType::STRUCT) {
		auto &children = StructVector::GetEntries(vector);
		for (auto &child : children) {
			InitializeValidities(*child, capacity);
		}
	}
}

static void RecursiveFlatten(Vector &vector, idx_t &count) {

	if (vector.GetVectorType() != VectorType::FLAT_VECTOR) {
		vector.Flatten(count);
	}

	auto internal_type = vector.GetType().InternalType();
	if (internal_type == PhysicalType::LIST) {
		auto &child_vector = ListVector::GetEntry(vector);
		auto child_vector_count = ListVector::GetListSize(vector);
		RecursiveFlatten(child_vector, child_vector_count);
	} else if (internal_type == PhysicalType::STRUCT) {
		auto &children = StructVector::GetEntries(vector);
		for (auto &child : children) {
			RecursiveFlatten(*child, count);
		}
	}
}

struct ListBindData : public FunctionData {
	explicit ListBindData(const LogicalType &stype_p);
	~ListBindData() override;

	LogicalType stype;
	WriteDataToSegment write_data_to_segment;
	ReadDataFromSegment read_data_from_segment;
	CopyDataFromSegment copy_data_from_segment;

	unique_ptr<FunctionData> Copy() const override {
		return make_unique<ListBindData>(stype);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = (const ListBindData &)other_p;
		return stype == other.stype;
	}
};

ListBindData::ListBindData(const LogicalType &stype_p) : stype(stype_p) {

	// always unnest once because the result vector is of type LIST
	auto type = ListType::GetChildType(stype_p);
	GetSegmentDataFunctions(write_data_to_segment, read_data_from_segment, copy_data_from_segment, type);
}

ListBindData::~ListBindData() {
}

struct ListAggState {
	LinkedList *linked_list;
	LogicalType *type;
	vector<AllocatedData> *owning_vector;
};

struct ListFunction {
	template <class STATE>
	static void Initialize(STATE *state) {
		state->linked_list = nullptr;
		state->type = nullptr;
		state->owning_vector = nullptr;
	}

	template <class STATE>
	static void Destroy(STATE *state) {
		D_ASSERT(state);
		if (state->linked_list) {
			delete state->linked_list;
			state->linked_list = nullptr;
		}
		if (state->type) {
			delete state->type;
			state->type = nullptr;
		}
		if (state->owning_vector) {
			state->owning_vector->clear();
			delete state->owning_vector;
			state->owning_vector = nullptr;
		}
	}
	static bool IgnoreNull() {
		return false;
	}
};

static void ListUpdateFunction(Vector inputs[], AggregateInputData &aggr_input_data, idx_t input_count,
                               Vector &state_vector, idx_t count) {
	D_ASSERT(input_count == 1);

	auto &input = inputs[0];
	UnifiedVectorFormat sdata;
	state_vector.ToUnifiedFormat(count, sdata);

	auto states = (ListAggState **)sdata.data;
	RecursiveFlatten(input, count);

	auto &list_bind_data = (ListBindData &)*aggr_input_data.bind_data;

	for (idx_t i = 0; i < count; i++) {
		auto state = states[sdata.sel->get_index(i)];
		if (!state->linked_list) {
			state->linked_list = new LinkedList(0, nullptr, nullptr);
			state->type = new LogicalType(input.GetType());
			state->owning_vector = new vector<AllocatedData>;
		}
		D_ASSERT(state->type);
		list_bind_data.write_data_to_segment.AppendRow(aggr_input_data.allocator, *state->owning_vector,
		                                               state->linked_list, input, i, count);
	}
}

static void ListCombineFunction(Vector &state, Vector &combined, AggregateInputData &aggr_input_data, idx_t count) {
	UnifiedVectorFormat sdata;
	state.ToUnifiedFormat(count, sdata);
	auto states_ptr = (ListAggState **)sdata.data;

	auto &list_bind_data = (ListBindData &)*aggr_input_data.bind_data;

	auto combined_ptr = FlatVector::GetData<ListAggState *>(combined);
	for (idx_t i = 0; i < count; i++) {
		auto state = states_ptr[sdata.sel->get_index(i)];
		if (!state->linked_list) {
			// NULL, no need to append.
			continue;
		}
		D_ASSERT(state->type);
		D_ASSERT(state->owning_vector);

		if (!combined_ptr[i]->linked_list) {
			combined_ptr[i]->linked_list = new LinkedList(0, nullptr, nullptr);
			combined_ptr[i]->owning_vector = new vector<AllocatedData>;
			combined_ptr[i]->type = new LogicalType(*state->type);
		}
		auto owning_vector = combined_ptr[i]->owning_vector;

		// copy the linked list of the state
		auto copied_linked_list = LinkedList(state->linked_list->total_capacity, nullptr, nullptr);
		list_bind_data.copy_data_from_segment.CopyLinkedList(state->linked_list, copied_linked_list,
		                                                     aggr_input_data.allocator, *owning_vector);

		// append the copied linked list to the combined state
		if (combined_ptr[i]->linked_list->last_segment) {
			combined_ptr[i]->linked_list->last_segment->next = copied_linked_list.first_segment;
		} else {
			combined_ptr[i]->linked_list->first_segment = copied_linked_list.first_segment;
		}
		combined_ptr[i]->linked_list->last_segment = copied_linked_list.last_segment;
		combined_ptr[i]->linked_list->total_capacity += copied_linked_list.total_capacity;
	}
}

static void ListFinalize(Vector &state_vector, AggregateInputData &aggr_input_data, Vector &result, idx_t count,
                         idx_t offset) {
	UnifiedVectorFormat sdata;
	state_vector.ToUnifiedFormat(count, sdata);
	auto states = (ListAggState **)sdata.data;

	D_ASSERT(result.GetType().id() == LogicalTypeId::LIST);

	auto &mask = FlatVector::Validity(result);
	auto result_data = FlatVector::GetData<list_entry_t>(result);
	size_t total_len = ListVector::GetListSize(result);

	auto &list_bind_data = (ListBindData &)*aggr_input_data.bind_data;

	for (idx_t i = 0; i < count; i++) {

		auto state = states[sdata.sel->get_index(i)];
		const auto rid = i + offset;
		if (!state->linked_list) {
			mask.SetInvalid(rid);
			continue;
		}

		// set the length and offset of this list in the result vector
		auto total_capacity = state->linked_list->total_capacity;
		result_data[rid].length = total_capacity;
		result_data[rid].offset = total_len;
		total_len += total_capacity;

		D_ASSERT(state->type);

		Vector aggr_vector(*state->type, total_capacity);
		// FIXME: this is a workaround because the constructor of a vector does not set the size
		// of the validity mask, and by default it is set to STANDARD_VECTOR_SIZE
		// ListVector::Reserve only increases the validity mask, if (to_reserve > capacity),
		// which will not be the case if the value passed to the constructor of aggr_vector
		// is greater than to_reserve
		InitializeValidities(aggr_vector, total_capacity);

		idx_t total_count = 0;
		list_bind_data.read_data_from_segment.BuildListVector(state->linked_list, aggr_vector, total_count);
		ListVector::Append(result, aggr_vector, total_capacity);

		// now destroy the state (for parallel destruction)
		ListFunction::Destroy<ListAggState>(state);
	}
}

unique_ptr<FunctionData> ListBindFunction(ClientContext &context, AggregateFunction &function,
                                          vector<unique_ptr<Expression>> &arguments) {
	D_ASSERT(arguments.size() == 1);
	D_ASSERT(function.arguments.size() == 1);

	if (arguments[0]->return_type.id() == LogicalTypeId::UNKNOWN) {
		function.arguments[0] = LogicalTypeId::UNKNOWN;
		function.return_type = LogicalType::SQLNULL;
		return nullptr;
	}

	function.return_type = LogicalType::LIST(arguments[0]->return_type);
	return make_unique<ListBindData>(function.return_type);
}

void ListFun::RegisterFunction(BuiltinFunctions &set) {
	auto agg =
	    AggregateFunction("list", {LogicalType::ANY}, LogicalTypeId::LIST, AggregateFunction::StateSize<ListAggState>,
	                      AggregateFunction::StateInitialize<ListAggState, ListFunction>, ListUpdateFunction,
	                      ListCombineFunction, ListFinalize, nullptr, ListBindFunction,
	                      AggregateFunction::StateDestroy<ListAggState, ListFunction>, nullptr, nullptr);
	set.AddFunction(agg);
	agg.name = "array_agg";
	set.AddFunction(agg);
}

} // namespace duckdb
